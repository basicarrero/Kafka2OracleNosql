final String tableName = "coches"
final String myJson = "{\"matricula\":1234, \"partes\":{\"num_ruedas\":[8,8,5,4], \"motor\":\"V8\"}}"

println(JsonUtil.inferSchema(myJson, tableName))
