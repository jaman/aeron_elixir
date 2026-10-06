#include <erl_nif.h>

static ERL_NIF_TERM add(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifSInt64 a;
    ErlNifSInt64 b;

    (void)argc;
    if (!enif_get_int64(env, argv[0], &a) || !enif_get_int64(env, argv[1], &b)) {
        return enif_make_badarg(env);
    }

    return enif_make_int64(env, (ErlNifSInt64)((ErlNifUInt64)a + (ErlNifUInt64)b));
}

static ErlNifFunc funcs[] = {{"add", 2, add, 0}};

ERL_NIF_INIT(Elixir.NifCeiling.C, funcs, NULL, NULL, NULL, NULL)
