package avt.nosql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;


public class JsonUtil {

  private static JsonNode parse(String json) throws IOException {
    return new ObjectMapper().readValue(json, JsonNode.class);
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

  public static class modelNode {
    private LinkedHashMap<String, modelNode> _childens;
    private String _type;
    private Boolean _isRoot;

    modelNode(String type){
      _isRoot = false;
      _type = type.toUpperCase();
      _childens = new LinkedHashMap<>();
    }

    void addChild(String name, modelNode child) { _childens.put(name, child); }

    void setType(String t) { _type = t.toUpperCase(); }

    String getType() { return _type; }

    modelNode getChild(String k) { return _childens.get(k); }

    private Set<Map.Entry<String, modelNode>> getChildSet() { return _childens.entrySet(); }

    boolean isRoot() { return _isRoot; }

    boolean isNull() { return "NULL".equals(_type); }

    void setTableName(String name) {
      _type = "CREATE TABLE " + name;
      _isRoot =  true;
    }

    @Override
    public String toString() {
      return toString(0);
    }

    String toString(int tabs) {
      if ("RECORD".equals(_type) || isRoot()) {
        StringBuilder str = new StringBuilder();
        str.append(_type);
        str.append(" (");
        for (Map.Entry<String, modelNode> ch : _childens.entrySet()) {
          if (tabs > 0)
            str.append("\n");
          for (int c = tabs; c > 0; c--)
            str.append("\t");
          str.append(ch.getKey());
          str.append(" ");
          str.append((tabs > 0) ? ch.getValue().toString(tabs + 1) : ch.getValue().toString());
          str.append(",");
        }
        str.setCharAt(str.length() - 1, ')');
        return str.toString();
      } else return _type;
    }

    boolean hasNullTypes() {
      return isNull() || _childHasNull();
    }

    private boolean _childHasNull() {
      for (Map.Entry<String, modelNode> ch : _childens.entrySet()) {
        if (ch.getValue().hasNullTypes())
          return true;
      }
      return false;
    }

    void mergeTypes(modelNode newNode) {
      if (newNode != null) {
        if (isNull() && ! newNode.isNull()){
          setType(newNode.getType());
          System.out.println("Nuevo TIPO añadido: " + newNode.getType());
        }
        for (Map.Entry<String, modelNode> ch : _childens.entrySet())
          if (ch.getValue().getType().equals("RECORD"))
            ch.getValue().mergeTypes(newNode.getChild(ch.getKey()));

        for (Map.Entry<String, modelNode> ch : newNode.getChildSet()) {
          if ( ! _childens.containsKey(ch.getKey()) && ! ch.getValue().isNull()) {
            addChild(ch.getKey(), ch.getValue());
            System.out.println("Nuevo CAMPO añadido: " + ch.getKey() + " " + ch.getValue().getType());
          }
        }
      }
    }
  }

  public static class JsonSchemaVisitor extends JsonTreeVisitor<modelNode> {
//    private boolean objectsToRecords;
//
//    JsonSchemaVisitor() {
//      this.objectsToRecords  = true;
//  }
//
//    boolean isObjectsToRecords() {
//      return objectsToRecords;
//    }
//
//    private JsonSchemaVisitor useMaps() {
//      this.objectsToRecords = false;
//      return this;
//    }

    @Override
    public modelNode object(ObjectNode object, Map<String, modelNode> fields) {
      modelNode modelObject = new modelNode("RECORD");
      for (Map.Entry<String, modelNode> r : fields.entrySet()) {
        modelObject.addChild(r.getKey(), r.getValue());
      }
      return modelObject;
    }

    @Override
    public modelNode array(ArrayNode array, List<modelNode> elements) {
      if (elements.size() > 0) return new modelNode("ARRAY(" + elements.get(0).toString() + ")");
      else return new modelNode("NULL");
    }

    @Override
    public modelNode binary(BinaryNode ignored) {
      return new modelNode("BINARY");
    }

    @Override
    public modelNode text(TextNode ignored) {
      return new modelNode("STRING");
    }

    @Override
    public modelNode number(NumericNode number) {
      return new modelNode("NUMBER");
    }

    @Override
    public modelNode bool(BooleanNode ignored) {
      return new modelNode("BOOLEAN");
    }

    @Override
    public modelNode nullNode(NullNode ignored) {
      return new modelNode("NULL");
    }

    @Override
    public modelNode missing(MissingNode ignored) {
      throw new UnsupportedOperationException("MissingNode is not supported.");
    }
  }

  public static String inferSchema(final Stream<String> in, String tableName) throws IOException {
    modelNode model = null;
    JsonSchemaVisitor visitor = new JsonSchemaVisitor();
    Iterator<String> sit = in.iterator();
    if (sit.hasNext()) {
      JsonNode node = parse(sit.next());
      model = visit(node, visitor);
      model.setTableName(tableName);
      while (sit.hasNext() && model.hasNullTypes()) {
        model.mergeTypes(visit(parse(sit.next()), visitor));
      }
    }
    if ( !sit.hasNext() && model != null && model.hasNullTypes()) {
      System.err.println(model.toString(1));
      throw new IOException("Sample is not enought to infer all types!");
    }
    return (model != null) ? model.toString(1) : "";
  }

  public static String inferSchema(Path file, String tableName) throws IOException {
    Stream<String> stream = Files.lines(file);
    String res = inferSchema(stream, tableName);
    stream.close();
    return res;
  }
}
