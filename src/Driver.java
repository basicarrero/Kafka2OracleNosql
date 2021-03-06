import avt.nosql.JsonUtil;
import java.io.IOException;
import java.nio.file.Paths;

public class Driver {
    public static void main(String[] args) throws IOException {
        if (args.length == 2)
            System.out.println(JsonUtil.inferSchema(Paths.get(args[1]), args[0]));
        else
            System.out.println(JsonUtil.inferSchema(Paths.get("./twlist.txt"), "tweets"));
    }
}
