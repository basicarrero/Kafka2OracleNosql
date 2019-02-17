import java.nio.file.Paths

if (args.size() == 2 || args.size() == 3)
    println(JsonUtil.inferSchema(Paths.get(args[1]), args[0], (args.size() == 3) ? args[2] : null))
else
    println(JsonUtil.inferSchema(Paths.get("./sample.json"), "coches", "matricula"))
