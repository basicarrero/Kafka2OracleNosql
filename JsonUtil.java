import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class JsonUtil {

  public static class Field {
    private String type;
    Object content;
    Field(String type, Object content){
      this.type = type;
      this.content = content;
    }

    String getType() {
      return type;
    }

    String getContent() {
      return toString();
    }

    boolean isComplexField() {
      return "ARRAY".equals(type) || "RECORD".equals(type);
    }
    @Override
    public String toString() {
      return content.toString();
    }
  }

  private static final JsonFactory FACTORY = new JsonFactory();

  private static Iterator<JsonNode> parser(final InputStream stream) throws IOException {
    JsonParser parser = FACTORY.createParser(stream);
    parser.setCodec(new ObjectMapper());
    return parser.readValuesAs(JsonNode.class);
  }

  private static JsonNode parse(String json) throws IOException {
    return new ObjectMapper().readValue(json, JsonNode.class);
  }

  private static JsonNode parse(Path filePath) throws IOException {
    String fileContent = String.join(" ", Files.readAllLines(filePath, StandardCharsets.UTF_8));
    return new ObjectMapper().readValue(fileContent, JsonNode.class);
  }

  public abstract static class JsonTreeVisitor<T> {
    LinkedList<String> recordLevels = Lists.newLinkedList();

    public T object(ObjectNode object, Map<String, T> fields) {
      return null;
    }

    public T array(ArrayNode array, List<T> elements) {
      return null;
    }

    public T binary(BinaryNode binary) {
      return null;
    }

    public T text(TextNode text) {
      return null;
    }

    public T number(NumericNode number) {
      return null;
    }

    public T bool(BooleanNode bool) {
      return null;
    }

    public T missing(MissingNode missing) {
      return null;
    }

    public T nullNode(NullNode nullNode) {
      return null;
    }
  }

  private static <T> T visit(JsonNode node, JsonTreeVisitor<T> visitor) {
    switch (node.getNodeType()) {
      case OBJECT:
        Preconditions.checkArgument(node instanceof ObjectNode,
            "Expected instance of ObjectNode: " + node);

        // use LinkedHashMap to preserve field order
        Map<String, T> fields = Maps.newLinkedHashMap();

        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();

          visitor.recordLevels.push(entry.getKey());
          fields.put(entry.getKey(), visit(entry.getValue(), visitor));
          visitor.recordLevels.pop();
        }

        return visitor.object((ObjectNode) node, fields);

      case ARRAY:
        Preconditions.checkArgument(node instanceof ArrayNode,
            "Expected instance of ArrayNode: " + node);

        List<T> elements = Lists.newArrayListWithExpectedSize(node.size());

        for (JsonNode element : node) {
          elements.add(visit(element, visitor));
        }

        return visitor.array((ArrayNode) node, elements);

      case BINARY:
        Preconditions.checkArgument(node instanceof BinaryNode,
            "Expected instance of BinaryNode: " + node);
        return visitor.binary((BinaryNode) node);

      case STRING:
        Preconditions.checkArgument(node instanceof TextNode,
            "Expected instance of TextNode: " + node);

        return visitor.text((TextNode) node);

      case NUMBER:
        Preconditions.checkArgument(node instanceof NumericNode,
            "Expected instance of NumericNode: " + node);

        return visitor.number((NumericNode) node);

      case BOOLEAN:
        Preconditions.checkArgument(node instanceof BooleanNode,
            "Expected instance of BooleanNode: " + node);

        return visitor.bool((BooleanNode) node);

      case MISSING:
        Preconditions.checkArgument(node instanceof MissingNode,
            "Expected instance of MissingNode: " + node);

        return visitor.missing((MissingNode) node);

      case NULL:
        Preconditions.checkArgument(node instanceof NullNode,
            "Expected instance of NullNode: " + node);

        return visitor.nullNode((NullNode) node);

      default:
        throw new IllegalArgumentException(
            "Unknown node type: " + node.getNodeType() + ": " + node);
    }
  }

  public static class JsonSchemaVisitor extends JsonTreeVisitor<Field> {

    private boolean objectsToRecords;

    JsonSchemaVisitor() {
      this.objectsToRecords  = true;
  }

    boolean isObjectsToRecords() {
      return objectsToRecords;
    }

    private JsonSchemaVisitor useMaps() {
      this.objectsToRecords = false;
      return this;
    }

    @Override
    public Field object(ObjectNode object, Map<String, Field> fields) {
      // TODO: Implementar seleccíon de tablas hijas o registros
      if ( !isObjectsToRecords() && recordLevels.size() < 1) { /* TODO */ }
      StringBuilder content = new StringBuilder();
      int counter = 1;
      for (Map.Entry<String, Field> r : fields.entrySet()) {
        Field f = r.getValue();
        content.append(r.getKey()).append(" ").append(f.getType());
        if (f.isComplexField()) {
          content.append(" (").append(f.getContent()).append(")");
        }
        if (counter < fields.size()) content.append(", ");
        counter ++;
      }

      return new Field("RECORD", content.toString());
    }

    @Override
    public Field array(ArrayNode array, List<Field> elements) {
      // TODO: Distinguir entre array y map según el tipo de los elementos
      return new Field("ARRAY", elements.get(0).getType());
    }

    @Override
    public Field binary(BinaryNode ignored) {
      return new Field("BINARY", ignored.toString());
    }

    @Override
    public Field text(TextNode ignored) {
      return new Field("STRING", ignored.toString());
    }

    @Override
    public Field number(NumericNode number) {
      return new Field("NUMBER", number.toString());
    }

    @Override
    public Field bool(BooleanNode ignored) {
      return new Field("BOOLEAN", ignored.toString());
    }

    @Override
    public Field nullNode(NullNode ignored) {
      throw new UnsupportedOperationException("NullNode is not supported.");
    }

    @Override
    public Field missing(MissingNode ignored) {
      throw new UnsupportedOperationException("MissingNode is not supported.");
    }
  }

  private static String inferSchema(JsonNode node, String tableName) {
    Field tableSchema = visit(node, new JsonSchemaVisitor().useMaps());
    return "CREATE TABLE " + tableName + " (" + tableSchema.getContent() + ");";
  }

  public static String inferSchema(String rawJson, String tableName) throws IOException {
    return JsonUtil.inferSchema(JsonUtil.parse(rawJson), tableName);
  }
}
