#include <erl_nif.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define FRAME_ALIGNMENT 32
#define HEADER_LENGTH 32
#define DELIVERED_HEADER_LENGTH 44
#define PARTITION_COUNT 3
#define TAIL_COUNTERS_OFFSET 0
#define ACTIVE_TERM_COUNT_OFFSET 24
#define HDR_TYPE_DATA 0x01
#define HDR_TYPE_PAD 0x00
#define FLAGS_BEGIN_END 0xC0
#define FLAG_BEGIN 0x80
#define FLAG_END 0x40

#define OFFER_BACK_PRESSURED (-2)
#define OFFER_ADMIN_ACTION (-3)
#define OFFER_CLOSED (-4)
#define OFFER_MAX_POSITION_EXCEEDED (-5)
#define OFFER_PAYLOAD_TOO_LONG (-6)
#define ADMIN_ACTION_RETRY_LIMIT 8
#define MAX_COLLECTED_MESSAGES 2048

#define BROADCAST_RECORD_HEADER_LENGTH 8
#define BROADCAST_RECORD_ALIGNMENT 8
#define BROADCAST_PADDING_MSG_TYPE_ID (-1)
#define BROADCAST_TAIL_INTENT_OFFSET 0
#define BROADCAST_TAIL_OFFSET 8
#define MAX_BROADCAST_RECORDS 64

#define PATH_BUFFER_LENGTH 4096
#define HEAP_BINARY_LIMIT 64
#define MAX_IODATA_SEGMENTS 16

typedef struct mapped_region {
    uintptr_t address;
    size_t length;
    int32_t closed;
    struct mapped_region *parent;
} mapped_region;

typedef struct {
    int64_t metadata;
    int64_t log_base;
    int64_t term_length;
    int64_t session_id;
    int64_t stream_id;
    int64_t initial_term_id;
    int64_t position_bits_to_shift;
    int64_t max_possible_position;
    int64_t limit_address;
    int64_t closed_address;
    int64_t max_payload_length;
} publication_geometry;

typedef struct {
    int64_t log_base;
    int64_t metadata;
    int64_t term_length;
    int64_t initial_term_id;
    int64_t position_bits_to_shift;
    int64_t subscriber_position_address;
    int64_t closed_address;
} image_geometry;

typedef struct {
    const unsigned char *data;
    size_t length;
} iodata_segment;

typedef struct {
    const unsigned char *bytes;
    size_t length;
    ERL_NIF_TERM iodata;
    int is_iodata;
    int overflowed;
    size_t segment_count;
    iodata_segment segments[MAX_IODATA_SEGMENTS];
    unsigned char single_bytes[MAX_IODATA_SEGMENTS];
} payload_source;

typedef struct {
    uintptr_t term_base;
    size_t start_offset;
    size_t end_offset;
    size_t total;
    int64_t end_position;
} message_span;

typedef struct {
    size_t offset;
    int32_t length;
    int32_t type_id;
    int64_t cursor;
    int64_t next;
} broadcast_record;

typedef enum { SHAPE_PAYLOAD, SHAPE_FRAMED, SHAPE_FRAMED_POSITIONS } collect_shape;

static ErlNifResourceType *region_type;
static int64_t live_regions;

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_failed;
static ERL_NIF_TERM atom_lapped;
static ERL_NIF_TERM atom_parent_already_set;
static ERL_NIF_TERM atom_nil;

static void region_dtor(ErlNifEnv *env, void *object) {
    mapped_region *region = object;
    (void)env;
    munmap((void *)region->address, region->length);
    __atomic_fetch_sub(&live_regions, 1, __ATOMIC_RELAXED);
    if (region->parent != NULL) {
        enif_release_resource(region->parent);
    }
}

static inline size_t align_up(size_t value) { return (value + FRAME_ALIGNMENT - 1) & ~(size_t)(FRAME_ALIGNMENT - 1); }

static inline size_t broadcast_align(size_t value) {
    return (value + BROADCAST_RECORD_ALIGNMENT - 1) & ~(size_t)(BROADCAST_RECORD_ALIGNMENT - 1);
}

static inline int32_t wrapping_sub_i32(int32_t left, int32_t right) { return (int32_t)((uint32_t)left - (uint32_t)right); }

static inline int32_t wrapping_add_i32(int32_t left, int32_t right) { return (int32_t)((uint32_t)left + (uint32_t)right); }

static inline int64_t shift_left_i64(int64_t value, unsigned bits) { return (int64_t)((uint64_t)value << bits); }

static inline int64_t floor_mod(int64_t value, int64_t divisor) {
    int64_t remainder = value % divisor;
    return remainder < 0 ? remainder + divisor : remainder;
}

static inline int64_t pack_tail(int32_t term_id, int32_t term_offset) {
    return (int64_t)(((uint64_t)(uint32_t)term_id << 32) | (uint64_t)(uint32_t)term_offset);
}

static inline int32_t term_id_of(int64_t raw_tail) { return (int32_t)(raw_tail >> 32); }

static inline int64_t term_offset_of(int64_t raw_tail) { return raw_tail & 0xFFFFFFFF; }

static inline void store_plain_i32(uintptr_t address, int32_t value) { memcpy((void *)address, &value, sizeof value); }

static inline void store_plain_i64(uintptr_t address, int64_t value) { memcpy((void *)address, &value, sizeof value); }

static inline void store_plain_u16(uintptr_t address, uint16_t value) { memcpy((void *)address, &value, sizeof value); }

static inline void store_plain_u8(uintptr_t address, uint8_t value) { *(uint8_t *)address = value; }

static inline int32_t load_plain_i32(uintptr_t address) {
    int32_t value;
    memcpy(&value, (const void *)address, sizeof value);
    return value;
}

static inline int64_t load_plain_i64(uintptr_t address) {
    int64_t value;
    memcpy(&value, (const void *)address, sizeof value);
    return value;
}

static inline uint16_t load_plain_u16(uintptr_t address) {
    uint16_t value;
    memcpy(&value, (const void *)address, sizeof value);
    return value;
}

static inline uint8_t load_plain_u8(uintptr_t address) { return *(const uint8_t *)address; }

static inline void store_release_i32(uintptr_t address, int32_t value) {
    __atomic_store_n((int32_t *)address, value, __ATOMIC_RELEASE);
}

static inline void store_release_i64(uintptr_t address, int64_t value) {
    __atomic_store_n((int64_t *)address, value, __ATOMIC_RELEASE);
}

static inline int32_t load_acquire_i32(uintptr_t address) { return __atomic_load_n((int32_t *)address, __ATOMIC_ACQUIRE); }

static inline int64_t load_acquire_i64(uintptr_t address) { return __atomic_load_n((int64_t *)address, __ATOMIC_ACQUIRE); }

static inline int64_t fetch_add_i64(uintptr_t address, int64_t delta) {
    return __atomic_fetch_add((int64_t *)address, delta, __ATOMIC_ACQ_REL);
}

static inline int cas_i64(uintptr_t address, int64_t *expected, int64_t desired) {
    return __atomic_compare_exchange_n((int64_t *)address, expected, desired, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

static inline int cas_i32(uintptr_t address, int32_t expected, int32_t desired) {
    return __atomic_compare_exchange_n((int32_t *)address, &expected, desired, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

static ERL_NIF_TERM make_binary_string(ErlNifEnv *env, const char *text) {
    ERL_NIF_TERM term;
    size_t length = strlen(text);
    unsigned char *data = enif_make_new_binary(env, length, &term);
    memcpy(data, text, length);
    return term;
}

static ERL_NIF_TERM error_string(ErlNifEnv *env, const char *reason) {
    return enif_make_tuple2(env, atom_error, make_binary_string(env, reason));
}

static ERL_NIF_TERM raise_reason(ErlNifEnv *env, const char *reason) {
    return enif_raise_exception(env, enif_make_atom(env, reason));
}

static int get_i64(ErlNifEnv *env, ERL_NIF_TERM term, int64_t *value) {
    ErlNifSInt64 decoded;
    if (!enif_get_int64(env, term, &decoded)) {
        return 0;
    }
    *value = (int64_t)decoded;
    return 1;
}

static int get_i32(ErlNifEnv *env, ERL_NIF_TERM term, int32_t *value) {
    int decoded;
    if (!enif_get_int(env, term, &decoded)) {
        return 0;
    }
    *value = (int32_t)decoded;
    return 1;
}

static int get_usize(ErlNifEnv *env, ERL_NIF_TERM term, size_t *value) {
    ErlNifUInt64 decoded;
    if (!enif_get_uint64(env, term, &decoded)) {
        return 0;
    }
    *value = (size_t)decoded;
    return 1;
}

static int get_region(ErlNifEnv *env, ERL_NIF_TERM term, mapped_region **region) {
    return enif_get_resource(env, term, region_type, (void **)region);
}

static const char *map_file(ErlNifEnv *env, ERL_NIF_TERM path_term, mapped_region *mapped) {
    ErlNifBinary path;
    char path_buffer[PATH_BUFFER_LENGTH];
    struct stat stat_buffer;

    if (!enif_inspect_iolist_as_binary(env, path_term, &path)) {
        return NULL;
    }
    if (path.size >= PATH_BUFFER_LENGTH) {
        return "path_too_long";
    }
    memcpy(path_buffer, path.data, path.size);
    path_buffer[path.size] = '\0';

    int fd = open(path_buffer, O_RDWR);
    if (fd < 0) {
        return "open_failed";
    }
    if (fstat(fd, &stat_buffer) < 0) {
        close(fd);
        return "fstat_failed";
    }
    if (stat_buffer.st_size == 0) {
        close(fd);
        return "empty_file";
    }

    size_t length = (size_t)stat_buffer.st_size;
    void *address = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (address == MAP_FAILED) {
        return "mmap_failed";
    }

    mapped->address = (uintptr_t)address;
    mapped->length = length;
    mapped->closed = 0;
    mapped->parent = NULL;
    return "";
}

static ERL_NIF_TERM wrap_region(ErlNifEnv *env, const mapped_region *mapped) {
    mapped_region *region = enif_alloc_resource(region_type, sizeof(mapped_region));
    *region = *mapped;
    __atomic_fetch_add(&live_regions, 1, __ATOMIC_RELAXED);
    ERL_NIF_TERM term = enif_make_resource(env, region);
    enif_release_resource(region);
    return term;
}

static ERL_NIF_TERM map_cnc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    mapped_region mapped;
    const char *failure = map_file(env, argv[0], &mapped);
    (void)argc;
    if (failure == NULL) {
        return enif_make_badarg(env);
    }
    if (failure[0] != '\0') {
        return error_string(env, failure);
    }
    return enif_make_tuple3(env, atom_ok, wrap_region(env, &mapped), enif_make_int64(env, (int64_t)mapped.address));
}

static ERL_NIF_TERM map_log(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    mapped_region mapped;
    const char *failure = map_file(env, argv[0], &mapped);
    (void)argc;
    if (failure == NULL) {
        return enif_make_badarg(env);
    }
    if (failure[0] != '\0') {
        return error_string(env, failure);
    }
    return enif_make_tuple4(env, atom_ok, wrap_region(env, &mapped), enif_make_int64(env, (int64_t)mapped.address),
                            enif_make_int64(env, (int64_t)mapped.length));
}

static ERL_NIF_TERM mapped_region_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
    return enif_make_int64(env, __atomic_load_n(&live_regions, __ATOMIC_RELAXED));
}

static ERL_NIF_TERM closed_flag_address(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    mapped_region *region;
    (void)argc;
    if (!get_region(env, argv[0], &region)) {
        return enif_make_badarg(env);
    }
    return enif_make_int64(env, (int64_t)(uintptr_t)&region->closed);
}

static ERL_NIF_TERM retain_parent_region(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    mapped_region *region;
    mapped_region *parent;
    (void)argc;
    if (!get_region(env, argv[0], &region) || !get_region(env, argv[1], &parent)) {
        return enif_make_badarg(env);
    }
    if (region->parent != NULL) {
        return enif_make_tuple2(env, atom_error, atom_parent_already_set);
    }
    enif_keep_resource(parent);
    region->parent = parent;
    return atom_ok;
}

static ERL_NIF_TERM read_metadata(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t resource;
    (void)argc;
    if (!get_i64(env, argv[0], &resource)) {
        return enif_make_badarg(env);
    }
    uintptr_t base = (uintptr_t)resource;

    ERL_NIF_TERM keys[] = {
        enif_make_atom(env, "cnc_version"),
        enif_make_atom(env, "to_driver_buffer_length"),
        enif_make_atom(env, "to_clients_buffer_length"),
        enif_make_atom(env, "counter_metadata_buffer_length"),
        enif_make_atom(env, "counter_values_buffer_length"),
        enif_make_atom(env, "error_log_buffer_length"),
        enif_make_atom(env, "client_liveness_timeout"),
        enif_make_atom(env, "start_timestamp"),
        enif_make_atom(env, "pid"),
        enif_make_atom(env, "file_page_size"),
    };
    ERL_NIF_TERM values[] = {
        enif_make_int(env, load_plain_i32(base + 0)),
        enif_make_int(env, load_plain_i32(base + 4)),
        enif_make_int(env, load_plain_i32(base + 8)),
        enif_make_int(env, load_plain_i32(base + 12)),
        enif_make_int(env, load_plain_i32(base + 16)),
        enif_make_int(env, load_plain_i32(base + 20)),
        enif_make_int64(env, load_plain_i64(base + 24)),
        enif_make_int64(env, load_plain_i64(base + 32)),
        enif_make_int64(env, load_plain_i64(base + 40)),
        enif_make_int(env, load_plain_i32(base + 48)),
    };
    ERL_NIF_TERM map;
    enif_make_map_from_arrays(env, keys, values, sizeof keys / sizeof keys[0], &map);
    return enif_make_tuple2(env, atom_ok, map);
}

static ERL_NIF_TERM get_buffer_address(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t resource;
    int64_t offset;
    (void)argc;
    if (!get_i64(env, argv[0], &resource) || !get_i64(env, argv[1], &offset)) {
        return enif_make_badarg(env);
    }
    return enif_make_tuple2(env, atom_ok, enif_make_int64(env, (int64_t)((uint64_t)resource + (uint64_t)offset)));
}

static ERL_NIF_TERM read_int32(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    (void)argc;
    if (!get_i64(env, argv[0], &address)) {
        return enif_make_badarg(env);
    }
    return enif_make_tuple2(env, atom_ok, enif_make_int(env, load_plain_i32((uintptr_t)address)));
}

static ERL_NIF_TERM read_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    (void)argc;
    if (!get_i64(env, argv[0], &address)) {
        return enif_make_badarg(env);
    }
    return enif_make_tuple2(env, atom_ok, enif_make_int64(env, load_plain_i64((uintptr_t)address)));
}

static ERL_NIF_TERM write_int32(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int32_t value;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i32(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }
    store_plain_i32((uintptr_t)address, value);
    return atom_ok;
}

static ERL_NIF_TERM write_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int64_t value;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i64(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }
    store_plain_i64((uintptr_t)address, value);
    return atom_ok;
}

static ERL_NIF_TERM write_int32_ordered(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int32_t value;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i32(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }
    store_release_i32((uintptr_t)address, value);
    return atom_ok;
}

static ERL_NIF_TERM write_int64_ordered(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int64_t value;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i64(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }
    store_release_i64((uintptr_t)address, value);
    return atom_ok;
}

static ERL_NIF_TERM atomic_get_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    (void)argc;
    if (!get_i64(env, argv[0], &address)) {
        return enif_make_badarg(env);
    }
    return enif_make_tuple2(env, atom_ok, enif_make_int64(env, load_acquire_i64((uintptr_t)address)));
}

static ERL_NIF_TERM atomic_fetch_add_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int64_t delta;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i64(env, argv[1], &delta)) {
        return enif_make_badarg(env);
    }
    return enif_make_tuple2(env, atom_ok, enif_make_int64(env, fetch_add_i64((uintptr_t)address, delta)));
}

static ERL_NIF_TERM atomic_cas_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int64_t expected;
    int64_t desired;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i64(env, argv[1], &expected) || !get_i64(env, argv[2], &desired)) {
        return enif_make_badarg(env);
    }
    int64_t witness = expected;
    if (cas_i64((uintptr_t)address, &witness, desired)) {
        return enif_make_tuple2(env, atom_ok, enif_make_int64(env, expected));
    }
    return enif_make_tuple2(env, atom_failed, enif_make_int64(env, witness));
}

static ERL_NIF_TERM read_binary(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    size_t length;
    size_t offset;
    ERL_NIF_TERM term;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_usize(env, argv[1], &length) || !get_usize(env, argv[2], &offset)) {
        return enif_make_badarg(env);
    }
    unsigned char *data = enif_make_new_binary(env, length, &term);
    memcpy(data, (const void *)((uintptr_t)address + offset), length);
    return enif_make_tuple2(env, atom_ok, term);
}

static ERL_NIF_TERM write_binary(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    size_t offset;
    ErlNifBinary data;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_usize(env, argv[1], &offset) ||
        !enif_inspect_iolist_as_binary(env, argv[2], &data)) {
        return enif_make_badarg(env);
    }
    memcpy((void *)((uintptr_t)address + offset), data.data, data.size);
    return atom_ok;
}

static int fragment_chain(uintptr_t term_base, size_t start_offset, size_t limit_offset, size_t *end_offset,
                          size_t *total) {
    size_t offset = start_offset;
    size_t sum = 0;

    while (offset < limit_offset) {
        uintptr_t frame_address = term_base + offset;
        int32_t frame_length = load_acquire_i32(frame_address);
        if (frame_length <= 0) {
            return 0;
        }
        if (load_plain_u16(frame_address + 6) == HDR_TYPE_PAD) {
            return 0;
        }

        sum += (size_t)(frame_length - HEADER_LENGTH);
        offset += align_up((size_t)frame_length);

        if (load_plain_u8(frame_address + 5) & FLAG_END) {
            *end_offset = offset;
            *total = sum;
            return 1;
        }
    }

    return 0;
}

static void write_data_header(uintptr_t address, int32_t frame_length, int32_t term_offset, int32_t session_id,
                              int32_t stream_id, int32_t term_id, int64_t reserved_value) {
    store_plain_i32(address + 0, -frame_length);
    store_plain_u8(address + 4, 0);
    store_plain_u8(address + 5, FLAGS_BEGIN_END);
    store_plain_u16(address + 6, HDR_TYPE_DATA);
    store_plain_i32(address + 8, term_offset);
    store_plain_i32(address + 12, session_id);
    store_plain_i32(address + 16, stream_id);
    store_plain_i32(address + 20, term_id);
    store_plain_i64(address + 24, reserved_value);
}

static void write_pad_header(uintptr_t address, int32_t padding_length, int32_t term_offset, int32_t session_id,
                             int32_t stream_id, int32_t term_id) {
    store_plain_i32(address + 0, -padding_length);
    store_plain_u8(address + 4, 0);
    store_plain_u8(address + 5, 0);
    store_plain_u16(address + 6, HDR_TYPE_PAD);
    store_plain_i32(address + 8, term_offset);
    store_plain_i32(address + 12, session_id);
    store_plain_i32(address + 16, stream_id);
    store_plain_i32(address + 20, term_id);
    store_plain_i64(address + 24, 0);
    store_release_i32(address + 0, padding_length);
}

static int broadcast_validate(uintptr_t tail_intent_address, int64_t cursor, int64_t capacity) {
    return (cursor + capacity) > load_acquire_i64(tail_intent_address);
}

static broadcast_record broadcast_record_at(uintptr_t base, int64_t cursor, uint64_t mask) {
    broadcast_record record;
    record.offset = (size_t)((uint64_t)cursor & mask);
    record.length = load_plain_i32(base + record.offset);
    record.type_id = load_plain_i32(base + record.offset + 4);
    record.cursor = cursor;
    record.next = cursor + (int64_t)broadcast_align((size_t)(record.length > 0 ? record.length : 0));
    return record;
}

static broadcast_record broadcast_unwrapped(broadcast_record record, uintptr_t base) {
    if (record.type_id != BROADCAST_PADDING_MSG_TYPE_ID) {
        return record;
    }

    broadcast_record unwrapped;
    unwrapped.offset = 0;
    unwrapped.length = load_plain_i32(base);
    unwrapped.type_id = load_plain_i32(base + 4);
    unwrapped.cursor = record.next;
    unwrapped.next = record.next + (int64_t)broadcast_align((size_t)(unwrapped.length > 0 ? unwrapped.length : 0));
    return unwrapped;
}

static int broadcast_readable(broadcast_record record, size_t capacity) {
    if (record.length < BROADCAST_RECORD_HEADER_LENGTH) {
        return 0;
    }
    return record.offset + (size_t)record.length <= capacity;
}

static ERL_NIF_TERM broadcast_lapped(ErlNifEnv *env) { return enif_make_tuple2(env, atom_error, atom_lapped); }

static ERL_NIF_TERM broadcast_receive(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    int64_t address;
    int64_t capacity_arg;
    int64_t position;
    int64_t limit_arg;
    (void)argc;
    if (!get_i64(env, argv[0], &address) || !get_i64(env, argv[1], &capacity_arg) ||
        !get_i64(env, argv[2], &position) || !get_i64(env, argv[3], &limit_arg)) {
        return enif_make_badarg(env);
    }
    if (capacity_arg <= 0) {
        return raise_reason(env, "invalid_broadcast_capacity");
    }
    size_t capacity = (size_t)capacity_arg;
    if ((capacity & (capacity - 1)) != 0) {
        return raise_reason(env, "invalid_broadcast_capacity");
    }

    uintptr_t base = (uintptr_t)address;
    uintptr_t trailer = base + capacity;
    uintptr_t tail_intent_address = trailer + BROADCAST_TAIL_INTENT_OFFSET;
    uintptr_t tail_address = trailer + BROADCAST_TAIL_OFFSET;
    uint64_t mask = (uint64_t)(capacity - 1);

    size_t limit = (size_t)(limit_arg > 0 ? limit_arg : 0);
    if (limit > MAX_BROADCAST_RECORDS) {
        limit = MAX_BROADCAST_RECORDS;
    }

    ERL_NIF_TERM terms[MAX_BROADCAST_RECORDS];
    size_t count = 0;
    int64_t next_record = position;

    while (count < limit) {
        int64_t tail = load_acquire_i64(tail_address);
        int64_t cursor = next_record;
        if (tail <= cursor) {
            break;
        }
        if (!broadcast_validate(tail_intent_address, cursor, capacity_arg)) {
            return broadcast_lapped(env);
        }

        broadcast_record record = broadcast_unwrapped(broadcast_record_at(base, cursor, mask), base);
        if (!broadcast_readable(record, capacity)) {
            return broadcast_lapped(env);
        }

        size_t payload_length = (size_t)record.length - BROADCAST_RECORD_HEADER_LENGTH;
        ERL_NIF_TERM payload;
        unsigned char *destination = enif_make_new_binary(env, payload_length, &payload);
        if (payload_length > 0) {
            memcpy(destination, (const void *)(base + record.offset + BROADCAST_RECORD_HEADER_LENGTH), payload_length);
        }

        if (!broadcast_validate(tail_intent_address, record.cursor, capacity_arg)) {
            return broadcast_lapped(env);
        }

        terms[count] = enif_make_tuple2(env, enif_make_int(env, record.type_id), payload);
        count += 1;
        next_record = record.next;
    }

    return enif_make_tuple3(env, atom_ok, enif_make_list_from_array(env, terms, (unsigned)count),
                            enif_make_int64(env, next_record));
}

static int is_closed(int64_t closed_address) { return load_acquire_i32((uintptr_t)closed_address) != 0; }

static int read_publication_geometry(ErlNifBinary *bytes, publication_geometry *geometry) {
    if (bytes->size != sizeof(publication_geometry)) {
        return 0;
    }
    memcpy(geometry, bytes->data, sizeof(publication_geometry));
    return 1;
}

static int read_image_geometry(ErlNifBinary *bytes, image_geometry *geometry) {
    if (bytes->size != sizeof(image_geometry)) {
        return 0;
    }
    memcpy(geometry, bytes->data, sizeof(image_geometry));
    return 1;
}

static void rotate_log(uintptr_t metadata, int32_t term_count, int32_t term_id) {
    int32_t next_term_id = wrapping_add_i32(term_id, 1);
    int32_t next_term_count = wrapping_add_i32(term_count, 1);
    size_t next_index = (size_t)floor_mod(next_term_count, PARTITION_COUNT);
    int32_t expected_term_id = wrapping_sub_i32(next_term_id, PARTITION_COUNT);
    uintptr_t next_tail_address = metadata + TAIL_COUNTERS_OFFSET + next_index * 8;

    for (;;) {
        int64_t raw_tail = load_acquire_i64(next_tail_address);
        if (expected_term_id != term_id_of(raw_tail)) {
            break;
        }
        if (cas_i64(next_tail_address, &raw_tail, pack_tail(next_term_id, 0))) {
            break;
        }
    }

    cas_i32(metadata + ACTIVE_TERM_COUNT_OFFSET, term_count, next_term_count);
}

static void add_segment(payload_source *source, const unsigned char *data, size_t length) {
    source->length += length;
    if (length == 0) {
        return;
    }
    if (source->segment_count == MAX_IODATA_SEGMENTS) {
        source->overflowed = 1;
        return;
    }
    source->segments[source->segment_count].data = data;
    source->segments[source->segment_count].length = length;
    source->segment_count += 1;
}

static void add_byte(payload_source *source, unsigned byte) {
    if (source->segment_count == MAX_IODATA_SEGMENTS) {
        source->length += 1;
        source->overflowed = 1;
        return;
    }
    source->single_bytes[source->segment_count] = (unsigned char)byte;
    add_segment(source, &source->single_bytes[source->segment_count], 1);
}

static int measure_iodata(ErlNifEnv *env, ERL_NIF_TERM term, payload_source *source) {
    ErlNifBinary binary;
    unsigned byte;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;

    if (enif_inspect_binary(env, term, &binary)) {
        add_segment(source, binary.data, binary.size);
        return 1;
    }
    if (enif_get_uint(env, term, &byte)) {
        if (byte > 255) {
            return 0;
        }
        add_byte(source, byte);
        return 1;
    }

    ERL_NIF_TERM remaining = term;
    while (enif_get_list_cell(env, remaining, &head, &tail)) {
        if (!measure_iodata(env, head, source)) {
            return 0;
        }
        remaining = tail;
    }

    if (enif_is_empty_list(env, remaining)) {
        return 1;
    }
    if (enif_inspect_binary(env, remaining, &binary)) {
        add_segment(source, binary.data, binary.size);
        return 1;
    }
    return 0;
}

static size_t write_iodata(ErlNifEnv *env, ERL_NIF_TERM term, unsigned char *destination) {
    ErlNifBinary binary;
    unsigned byte;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;

    if (enif_inspect_binary(env, term, &binary)) {
        memcpy(destination, binary.data, binary.size);
        return binary.size;
    }
    if (enif_get_uint(env, term, &byte)) {
        destination[0] = (unsigned char)byte;
        return 1;
    }

    size_t written = 0;
    ERL_NIF_TERM remaining = term;
    while (enif_get_list_cell(env, remaining, &head, &tail)) {
        written += write_iodata(env, head, destination + written);
        remaining = tail;
    }
    if (enif_inspect_binary(env, remaining, &binary)) {
        memcpy(destination + written, binary.data, binary.size);
        written += binary.size;
    }
    return written;
}

static int payload_source_of(ErlNifEnv *env, ERL_NIF_TERM term, payload_source *source) {
    ErlNifBinary binary;
    if (enif_inspect_binary(env, term, &binary)) {
        source->bytes = binary.data;
        source->length = binary.size;
        source->is_iodata = 0;
        return 1;
    }
    source->length = 0;
    source->segment_count = 0;
    source->overflowed = 0;
    source->iodata = term;
    source->is_iodata = 1;
    return measure_iodata(env, term, source);
}

static void copy_payload(ErlNifEnv *env, const payload_source *source, unsigned char *destination) {
    if (!source->is_iodata) {
        memcpy(destination, source->bytes, source->length);
    } else if (source->overflowed) {
        write_iodata(env, source->iodata, destination);
    } else {
        for (size_t index = 0; index < source->segment_count; index++) {
            memcpy(destination, source->segments[index].data, source->segments[index].length);
            destination += source->segments[index].length;
        }
    }
}

static int64_t offer_once(ErlNifEnv *env, const publication_geometry *geometry, const payload_source *payload,
                          int64_t reserved_value) {
    if (is_closed(geometry->closed_address)) {
        return OFFER_CLOSED;
    }
    if (payload->length > (size_t)geometry->max_payload_length) {
        return OFFER_PAYLOAD_TOO_LONG;
    }

    uintptr_t metadata = (uintptr_t)geometry->metadata;
    uintptr_t log_base = (uintptr_t)geometry->log_base;
    int64_t term_length = geometry->term_length;
    int32_t initial_term_id = (int32_t)geometry->initial_term_id;
    unsigned bits = (unsigned)geometry->position_bits_to_shift;

    size_t frame_length = HEADER_LENGTH + payload->length;
    int64_t aligned = (int64_t)align_up(frame_length);

    int64_t limit = load_acquire_i64((uintptr_t)geometry->limit_address);
    int32_t term_count = load_acquire_i32(metadata + ACTIVE_TERM_COUNT_OFFSET);
    size_t index = (size_t)floor_mod(term_count, PARTITION_COUNT);
    uintptr_t tail_address = metadata + TAIL_COUNTERS_OFFSET + index * 8;
    int64_t raw_tail = load_acquire_i64(tail_address);
    int64_t term_offset = term_offset_of(raw_tail);
    int32_t term_id = term_id_of(raw_tail);
    int64_t position = shift_left_i64(wrapping_sub_i32(term_id, initial_term_id), bits) + term_offset;

    if (term_count != wrapping_sub_i32(term_id, initial_term_id)) {
        return OFFER_ADMIN_ACTION;
    }
    if (position >= limit) {
        return OFFER_BACK_PRESSURED;
    }

    int64_t reserved_raw_tail = fetch_add_i64(tail_address, aligned);
    int64_t reserved_offset = term_offset_of(reserved_raw_tail);
    int32_t reserved_term_id = term_id_of(reserved_raw_tail);
    int64_t resulting_offset = reserved_offset + aligned;
    uintptr_t term_base = log_base + index * (size_t)term_length;

    if (resulting_offset > term_length) {
        if (reserved_offset < term_length) {
            write_pad_header(term_base + (size_t)reserved_offset, (int32_t)(term_length - reserved_offset),
                             (int32_t)reserved_offset, (int32_t)geometry->session_id, (int32_t)geometry->stream_id,
                             reserved_term_id);
        }
        if ((position + term_offset) > geometry->max_possible_position) {
            return OFFER_MAX_POSITION_EXCEEDED;
        }
        rotate_log(metadata, term_count, reserved_term_id);
        return OFFER_ADMIN_ACTION;
    }

    uintptr_t frame_address = term_base + (size_t)reserved_offset;
    write_data_header(frame_address, (int32_t)frame_length, (int32_t)reserved_offset, (int32_t)geometry->session_id,
                      (int32_t)geometry->stream_id, reserved_term_id, reserved_value);
    copy_payload(env, payload, (unsigned char *)(frame_address + HEADER_LENGTH));
    store_release_i32(frame_address, (int32_t)frame_length);

    return shift_left_i64(wrapping_sub_i32(reserved_term_id, initial_term_id), bits) + resulting_offset;
}

static int64_t offer_with_admin_retry(ErlNifEnv *env, const publication_geometry *geometry,
                                      const payload_source *payload, int64_t reserved_value) {
    for (int attempt = 0; attempt < ADMIN_ACTION_RETRY_LIMIT; attempt++) {
        int64_t result = offer_once(env, geometry, payload, reserved_value);
        if (result != OFFER_ADMIN_ACTION) {
            return result;
        }
    }
    return OFFER_ADMIN_ACTION;
}

static ERL_NIF_TERM offer(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary geometry_bytes;
    int64_t reserved_value;
    publication_geometry geometry;
    payload_source payload;
    (void)argc;
    if (!enif_inspect_binary(env, argv[0], &geometry_bytes) || !get_i64(env, argv[2], &reserved_value)) {
        return enif_make_badarg(env);
    }
    if (!read_publication_geometry(&geometry_bytes, &geometry)) {
        return raise_reason(env, "invalid_publication_geometry");
    }
    if (!payload_source_of(env, argv[1], &payload)) {
        return raise_reason(env, "payload_not_iodata");
    }
    return enif_make_int64(env, offer_with_admin_retry(env, &geometry, &payload, reserved_value));
}

static ERL_NIF_TERM offer_n(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary geometry_bytes;
    ErlNifBinary payload_bytes;
    int64_t reserved_value;
    int64_t count;
    publication_geometry geometry;
    (void)argc;
    if (!enif_inspect_binary(env, argv[0], &geometry_bytes) || !enif_inspect_binary(env, argv[1], &payload_bytes) ||
        !get_i64(env, argv[2], &reserved_value) || !get_i64(env, argv[3], &count)) {
        return enif_make_badarg(env);
    }
    if (!read_publication_geometry(&geometry_bytes, &geometry)) {
        return raise_reason(env, "invalid_publication_geometry");
    }

    payload_source payload = {.bytes = payload_bytes.data, .length = payload_bytes.size, .is_iodata = 0};
    int64_t published = 0;
    while (published < count) {
        int64_t result = offer_with_admin_retry(env, &geometry, &payload, reserved_value);
        if (result == OFFER_CLOSED && published == 0) {
            return enif_make_int64(env, OFFER_CLOSED);
        }
        if (result < 0) {
            break;
        }
        published += 1;
    }
    return enif_make_int64(env, published);
}

static ERL_NIF_TERM offer_list(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary geometry_bytes;
    int64_t reserved_value;
    publication_geometry geometry;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;
    (void)argc;
    if (!enif_inspect_binary(env, argv[0], &geometry_bytes) || !get_i64(env, argv[2], &reserved_value)) {
        return enif_make_badarg(env);
    }
    if (!read_publication_geometry(&geometry_bytes, &geometry)) {
        return raise_reason(env, "invalid_publication_geometry");
    }

    int64_t published = 0;
    ERL_NIF_TERM remaining = argv[1];
    while (enif_get_list_cell(env, remaining, &head, &tail)) {
        payload_source payload;
        if (!payload_source_of(env, head, &payload)) {
            return raise_reason(env, "payload_not_iodata");
        }
        int64_t result = offer_with_admin_retry(env, &geometry, &payload, reserved_value);
        if (result == OFFER_CLOSED && published == 0) {
            return enif_make_int64(env, OFFER_CLOSED);
        }
        if (result < 0) {
            break;
        }
        published += 1;
        remaining = tail;
    }
    return enif_make_int64(env, published);
}

static ERL_NIF_TERM poll_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary geometry_bytes;
    int64_t fragment_limit;
    image_geometry geometry;
    (void)argc;
    if (!enif_inspect_binary(env, argv[0], &geometry_bytes) || !get_i64(env, argv[1], &fragment_limit)) {
        return enif_make_badarg(env);
    }
    if (!read_image_geometry(&geometry_bytes, &geometry)) {
        return raise_reason(env, "invalid_image_geometry");
    }
    if (is_closed(geometry.closed_address)) {
        return enif_make_int64(env, 0);
    }

    uintptr_t log_base = (uintptr_t)geometry.log_base;
    int64_t term_length = geometry.term_length;
    size_t term_length_size = (size_t)term_length;
    unsigned bits = (unsigned)geometry.position_bits_to_shift;
    uintptr_t subscriber_position_address = (uintptr_t)geometry.subscriber_position_address;

    int64_t start_position = load_acquire_i64(subscriber_position_address);
    int64_t position = start_position;
    int64_t count = 0;

    while (count < fragment_limit) {
        int32_t term_count = (int32_t)(position >> bits);
        size_t partition = (size_t)floor_mod(term_count, PARTITION_COUNT);
        int64_t term_offset = position & (term_length - 1);
        uintptr_t term_base = log_base + partition * term_length_size;
        size_t scan_offset = (size_t)term_offset;

        while (scan_offset < term_length_size && count < fragment_limit) {
            uintptr_t frame_address = term_base + scan_offset;
            int32_t frame_length = load_acquire_i32(frame_address);
            if (frame_length <= 0) {
                break;
            }
            scan_offset += align_up((size_t)frame_length);
            if (load_plain_u16(frame_address + 6) != HDR_TYPE_PAD) {
                count += 1;
            }
        }

        int64_t consumed = (int64_t)scan_offset - term_offset;
        if (consumed <= 0) {
            break;
        }
        position += consumed;
    }

    if (position != start_position) {
        store_release_i64(subscriber_position_address, position);
    }
    return enif_make_int64(env, count);
}

static size_t copy_span(unsigned char *destination, const message_span *span) {
    size_t offset = span->start_offset;
    size_t written = 0;

    while (offset < span->end_offset) {
        uintptr_t frame_address = span->term_base + offset;
        int32_t frame_length = load_acquire_i32(frame_address);
        size_t aligned = align_up((size_t)frame_length);

        if (load_plain_u16(frame_address + 6) != HDR_TYPE_PAD) {
            size_t payload_length = (size_t)(frame_length - HEADER_LENGTH);
            memcpy(destination + written, (const void *)(frame_address + HEADER_LENGTH), payload_length);
            written += payload_length;
        }

        offset += aligned;
    }

    return written;
}

static size_t last_frame_offset(const message_span *span) {
    size_t offset = span->start_offset;
    size_t last = offset;

    while (offset < span->end_offset) {
        last = offset;
        int32_t frame_length = load_acquire_i32(span->term_base + offset);
        offset += align_up((size_t)frame_length);
    }

    return last;
}

static ERL_NIF_TERM own_binary(ErlNifEnv *env, const message_span *span) {
    ERL_NIF_TERM term;
    copy_span(enif_make_new_binary(env, span->total, &term), span);
    return term;
}

static ERL_NIF_TERM collect_spans(ErlNifEnv *env, const message_span *spans, size_t count, size_t total_bytes) {
    ErlNifBinary batch;
    ERL_NIF_TERM terms[MAX_COLLECTED_MESSAGES];
    size_t offsets[MAX_COLLECTED_MESSAGES];
    size_t written = 0;

    if (count == 0) {
        return enif_make_list_from_array(env, NULL, 0);
    }
    if (count == 1 && spans[0].total <= HEAP_BINARY_LIMIT) {
        terms[0] = own_binary(env, &spans[0]);
        return enif_make_list_from_array(env, terms, 1);
    }
    if (!enif_alloc_binary(total_bytes, &batch)) {
        return raise_reason(env, "binary_alloc_failed");
    }

    for (size_t index = 0; index < count; index++) {
        offsets[index] = written;
        written += copy_span(batch.data + written, &spans[index]);
    }

    ERL_NIF_TERM batch_term = enif_make_binary(env, &batch);
    for (size_t index = 0; index < count; index++) {
        terms[index] = enif_make_sub_binary(env, batch_term, offsets[index], spans[index].total);
    }

    return enif_make_list_from_array(env, terms, (unsigned)count);
}

static ERL_NIF_TERM collect_framed_spans(ErlNifEnv *env, const message_span *spans, size_t count, size_t total_bytes,
                                         int32_t initial_term_id) {
    ErlNifBinary batch;
    ERL_NIF_TERM terms[MAX_COLLECTED_MESSAGES];
    size_t offsets[MAX_COLLECTED_MESSAGES];
    size_t written = 0;

    if (count == 0) {
        return enif_make_list_from_array(env, NULL, 0);
    }
    if (!enif_alloc_binary(total_bytes + count * DELIVERED_HEADER_LENGTH, &batch)) {
        return raise_reason(env, "binary_alloc_failed");
    }

    for (size_t index = 0; index < count; index++) {
        offsets[index] = written;
        memcpy(batch.data + written, (const void *)(spans[index].term_base + last_frame_offset(&spans[index])),
               HEADER_LENGTH);
        store_plain_i64((uintptr_t)(batch.data + written + HEADER_LENGTH), spans[index].end_position);
        store_plain_i32((uintptr_t)(batch.data + written + HEADER_LENGTH + 8), initial_term_id);
        written += DELIVERED_HEADER_LENGTH;
        written += copy_span(batch.data + written, &spans[index]);
    }

    ERL_NIF_TERM batch_term = enif_make_binary(env, &batch);
    for (size_t index = 0; index < count; index++) {
        ERL_NIF_TERM header = enif_make_sub_binary(env, batch_term, offsets[index], DELIVERED_HEADER_LENGTH);
        ERL_NIF_TERM payload =
            enif_make_sub_binary(env, batch_term, offsets[index] + DELIVERED_HEADER_LENGTH, spans[index].total);
        terms[index] = enif_make_tuple2(env, header, payload);
    }

    return enif_make_list_from_array(env, terms, (unsigned)count);
}

typedef struct {
    size_t count;
    size_t total_bytes;
    int64_t start_position;
    int64_t position;
} scan_result;

static scan_result scan_messages(const image_geometry *geometry, size_t limit, message_span *spans) {
    uintptr_t log_base = (uintptr_t)geometry->log_base;
    int64_t term_length = geometry->term_length;
    size_t term_length_size = (size_t)term_length;
    unsigned bits = (unsigned)geometry->position_bits_to_shift;
    scan_result result = {0, 0, 0, 0};

    result.start_position = load_acquire_i64((uintptr_t)geometry->subscriber_position_address);
    result.position = result.start_position;

    while (result.count < limit) {
        int32_t term_count = (int32_t)(result.position >> bits);
        size_t partition = (size_t)floor_mod(term_count, PARTITION_COUNT);
        int64_t term_offset = result.position & (term_length - 1);
        uintptr_t term_base = log_base + partition * term_length_size;
        size_t scan_offset = (size_t)term_offset;

        while (scan_offset < term_length_size && result.count < limit) {
            uintptr_t frame_address = term_base + scan_offset;
            int32_t frame_length = load_acquire_i32(frame_address);
            if (frame_length <= 0) {
                return result;
            }

            size_t aligned = align_up((size_t)frame_length);

            if (load_plain_u16(frame_address + 6) == HDR_TYPE_PAD) {
                scan_offset += aligned;
                result.position += (int64_t)aligned;
                continue;
            }

            uint8_t flags = load_plain_u8(frame_address + 5);
            size_t payload_length = (size_t)(frame_length - HEADER_LENGTH);

            if ((flags & FLAG_BEGIN) && (flags & FLAG_END)) {
                scan_offset += aligned;
                result.position += (int64_t)aligned;
                spans[result.count] =
                    (message_span){term_base, scan_offset - aligned, scan_offset, payload_length, result.position};
                result.count += 1;
                result.total_bytes += payload_length;
            } else if (flags & FLAG_BEGIN) {
                size_t chain_end;
                size_t chain_total;
                if (!fragment_chain(term_base, scan_offset, term_length_size, &chain_end, &chain_total)) {
                    return result;
                }
                size_t chain_start = scan_offset;
                result.position += (int64_t)(chain_end - scan_offset);
                scan_offset = chain_end;
                spans[result.count] = (message_span){term_base, chain_start, chain_end, chain_total, result.position};
                result.count += 1;
                result.total_bytes += chain_total;
            } else {
                return result;
            }
        }
    }

    return result;
}

static int read_image_arguments(ErlNifEnv *env, ERL_NIF_TERM geometry_term, image_geometry *geometry,
                                ERL_NIF_TERM *failure) {
    ErlNifBinary geometry_bytes;
    if (!enif_inspect_binary(env, geometry_term, &geometry_bytes)) {
        *failure = enif_make_badarg(env);
        return 0;
    }
    if (!read_image_geometry(&geometry_bytes, geometry)) {
        *failure = raise_reason(env, "invalid_image_geometry");
        return 0;
    }
    return 1;
}

static ERL_NIF_TERM collect_messages(ErlNifEnv *env, const ERL_NIF_TERM argv[], collect_shape shape) {
    int64_t fragment_limit;
    image_geometry geometry;
    ERL_NIF_TERM failure;
    message_span spans[MAX_COLLECTED_MESSAGES];

    if (!get_i64(env, argv[1], &fragment_limit)) {
        return enif_make_badarg(env);
    }
    if (!read_image_arguments(env, argv[0], &geometry, &failure)) {
        return failure;
    }
    if (is_closed(geometry.closed_address)) {
        return enif_make_tuple2(env, enif_make_int64(env, 0), enif_make_list_from_array(env, NULL, 0));
    }

    size_t limit = (size_t)(fragment_limit > 0 ? fragment_limit : 0);
    if (limit > MAX_COLLECTED_MESSAGES) {
        limit = MAX_COLLECTED_MESSAGES;
    }

    scan_result scan = scan_messages(&geometry, limit, spans);
    ERL_NIF_TERM messages;
    switch (shape) {
    case SHAPE_PAYLOAD:
        messages = collect_spans(env, spans, scan.count, scan.total_bytes);
        break;
    default:
        messages = collect_framed_spans(env, spans, scan.count, scan.total_bytes, (int32_t)geometry.initial_term_id);
        break;
    }

    if (shape != SHAPE_FRAMED_POSITIONS && scan.position != scan.start_position) {
        store_release_i64((uintptr_t)geometry.subscriber_position_address, scan.position);
    }

    return enif_make_tuple2(env, enif_make_int64(env, (int64_t)scan.count), messages);
}

static ERL_NIF_TERM poll_next(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    image_geometry geometry;
    ERL_NIF_TERM failure;
    message_span span;
    (void)argc;

    if (!read_image_arguments(env, argv[0], &geometry, &failure)) {
        return failure;
    }
    if (is_closed(geometry.closed_address)) {
        return atom_nil;
    }

    scan_result scan = scan_messages(&geometry, 1, &span);
    ERL_NIF_TERM message = scan.count == 0 ? atom_nil : own_binary(env, &span);

    if (scan.position != scan.start_position) {
        store_release_i64((uintptr_t)geometry.subscriber_position_address, scan.position);
    }

    return message;
}

static ERL_NIF_TERM poll_collect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    return collect_messages(env, argv, SHAPE_PAYLOAD);
}

static ERL_NIF_TERM poll_collect_framed(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    return collect_messages(env, argv, SHAPE_FRAMED);
}

static ERL_NIF_TERM peek_collect_framed(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    return collect_messages(env, argv, SHAPE_FRAMED_POSITIONS);
}

static int open_resources(ErlNifEnv *env, ErlNifResourceFlags flags) {
    region_type = enif_open_resource_type(env, NULL, "MappedRegion", region_dtor, flags, NULL);
    atom_ok = enif_make_atom(env, "ok");
    atom_error = enif_make_atom(env, "error");
    atom_failed = enif_make_atom(env, "failed");
    atom_lapped = enif_make_atom(env, "lapped");
    atom_parent_already_set = enif_make_atom(env, "parent_already_set");
    atom_nil = enif_make_atom(env, "nil");
    return region_type == NULL ? -1 : 0;
}

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info) {
    (void)priv_data;
    (void)load_info;
    return open_resources(env, ERL_NIF_RT_CREATE);
}

static int upgrade(ErlNifEnv *env, void **priv_data, void **old_priv_data, ERL_NIF_TERM load_info) {
    (void)priv_data;
    (void)old_priv_data;
    (void)load_info;
    return open_resources(env, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
}

static ErlNifFunc nif_functions[] = {
    {"map_cnc", 1, map_cnc, 0},
    {"map_log", 1, map_log, 0},
    {"mapped_region_count", 0, mapped_region_count, 0},
    {"closed_flag_address", 1, closed_flag_address, 0},
    {"retain_parent_region", 2, retain_parent_region, 0},
    {"read_metadata", 1, read_metadata, 0},
    {"get_buffer_address", 2, get_buffer_address, 0},
    {"read_int32", 1, read_int32, 0},
    {"read_int64", 1, read_int64, 0},
    {"write_int32", 2, write_int32, 0},
    {"write_int64", 2, write_int64, 0},
    {"write_int32_ordered", 2, write_int32_ordered, 0},
    {"write_int64_ordered", 2, write_int64_ordered, 0},
    {"atomic_get_int64", 1, atomic_get_int64, 0},
    {"atomic_fetch_add_int64", 2, atomic_fetch_add_int64, 0},
    {"atomic_cas_int64", 3, atomic_cas_int64, 0},
    {"read_binary", 3, read_binary, 0},
    {"write_binary", 3, write_binary, 0},
    {"offer", 3, offer, 0},
    {"offer_n", 4, offer_n, 0},
    {"offer_list", 3, offer_list, 0},
    {"poll_count", 2, poll_count, 0},
    {"poll_collect", 2, poll_collect, 0},
    {"poll_collect_framed", 2, poll_collect_framed, 0},
    {"peek_collect_framed", 2, peek_collect_framed, 0},
    {"poll_next", 1, poll_next, 0},
    {"broadcast_receive", 4, broadcast_receive, 0},
};

ERL_NIF_INIT(Elixir.AeronElixir.NIF, nif_functions, load, NULL, upgrade, NULL)
