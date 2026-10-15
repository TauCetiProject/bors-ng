# MIX_ENV=test BORS_DATABASE=postgresql mix run --no-start scripts/replay_diversity.exs INPUT OUTPUT
[input, output] = System.argv()
data = input |> File.read!() |> Jason.decode!()
result = BorsNG.Worker.Batcher.DiversityReplay.run(data)
File.write!(output, Jason.encode!(result, pretty: true) <> "\n")

IO.puts(
  "#{length(result.scenarios)} deterministic formation sensitivities; production disabled; counterfactual acceptance unknown"
)
