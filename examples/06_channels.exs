alias AeronElixir.ChannelUri
alias AeronElixir.Protocol.URI

IO.puts(ChannelUri.ipc())
IO.puts(ChannelUri.ipc(term_length: 1_048_576, alias: "orders"))
IO.puts(ChannelUri.udp(endpoint: "localhost:40123"))
IO.puts(ChannelUri.udp(endpoint: "224.0.1.1:40456", interface: "192.168.1.0/24", ttl: 16))
IO.puts(ChannelUri.udp(endpoint: "localhost:40123", mtu: 1408, reliable: false, session_id: 7))
IO.puts(ChannelUri.udp(control: "localhost:40124", control_mode: "dynamic"))
IO.inspect(ChannelUri.udp(term_length: 65_536), label: "missing endpoint")
IO.inspect(ChannelUri.udp(endpoint: "h:1", term_length: 1000), label: "bad term length")

{:ok, parsed} = URI.parse("aeron:udp?endpoint=localhost:40123|mtu=1408")
IO.inspect(parsed, label: "parsed")
