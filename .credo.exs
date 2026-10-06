%{
  configs: [
    %{
      name: "default",
      checks: [
        {Credo.Check.Readability.MaxLineLength, priority: :low, max_length: 120},
        {Credo.Check.Design.TagTODO, exit_status: 0}
      ]
    }
  ]
}