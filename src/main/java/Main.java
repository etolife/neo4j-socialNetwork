import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import javax.management.relation.Relation;
import javax.print.attribute.HashAttributeSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

public class Main
{
    public static void main(String[] args)
    {
        String dbPath = "dataBase/graphDb";
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));

        try (Transaction tx = graphDb.beginTx()) {

            System.out.println("Do you want to creat a social network kg? Y/N");
            Scanner input = new Scanner(System.in);
            String choose = input.next();
            if (choose.equalsIgnoreCase("y")) {
                // create
                createNodeOfSocialNetwork(graphDb);
            }

            // delete
            // deleteNodeOfSocialNetwork(graphDb);

            // update
            // updateNodeOfSocialNetwork(graphDb);

            // query
            System.out.println("Do you want to recommend movies to xiaoming? Y/N");
            Scanner input2 = new Scanner(System.in);
            String choose2 = input2.next();
            if (choose2.equalsIgnoreCase("y")) {
                queryNodeOfSocialNetwork(graphDb);
            }

            // commit transaction
            tx.success();
        }
    }

    public static void createNodeOfSocialNetwork(GraphDatabaseService graphDb)
    {
        IndexManager indexManager = graphDb.index();
        Index<Node> userIndex = indexManager.forNodes("users");

        Node xiaoming = graphDb.createNode(SocialNetworkLabel.USER);
        xiaoming.setProperty("name", "xiaoming");
        xiaoming.setProperty("age", 21);
        userIndex.add(xiaoming, "name", "xiaoming");

        Node xiaoqiang = graphDb.createNode(SocialNetworkLabel.USER);
        xiaoqiang.setProperty("name", "xiaoqiang");
        xiaoqiang.setProperty("age", 20);
        userIndex.add(xiaoqiang, "name", "xiaoqiang");

        Node xiaohong = graphDb.createNode(SocialNetworkLabel.USER);
        xiaohong.setProperty("name", "xiaohong");
        xiaohong.setProperty("age", 18);
        xiaohong.setProperty("gender", "female");
        userIndex.add(xiaohong, "name", "xiaohong");

        Node titanic = graphDb.createNode(SocialNetworkLabel.MOVIE);
        titanic.setProperty("name", "Titanic");
        titanic.setProperty("year", 1997);

        Node shawshank = graphDb.createNode(SocialNetworkLabel.MOVIE);
        shawshank.setProperty("name", "ShawShank");
        shawshank.setProperty("year", 1994);

        Node forrestgump = graphDb.createNode(SocialNetworkLabel.MOVIE);
        forrestgump.setProperty("name", "ForrestGump");
        forrestgump.setProperty("year", 1994);

        // create relationship
        Relationship r1 = xiaoming.createRelationshipTo(titanic, SocialNetworkRelationshipType.HAS_SEEN);
        r1.setProperty("stars", 5);

        Relationship r2 = xiaoqiang.createRelationshipTo(titanic, SocialNetworkRelationshipType.HAS_SEEN);
        r2.setProperty("stars", 4);
        Relationship r3 = xiaoqiang.createRelationshipTo(shawshank, SocialNetworkRelationshipType.HAS_SEEN);
        r3.setProperty("stars", 5);
        Relationship r4 = xiaoqiang.createRelationshipTo(forrestgump, SocialNetworkRelationshipType.HAS_SEEN);
        r4.setProperty("stars", 5);

        Relationship r5 = xiaohong.createRelationshipTo(shawshank, SocialNetworkRelationshipType.HAS_SEEN);
        r5.setProperty("stars", 5);
        Relationship r6 = xiaohong.createRelationshipTo(forrestgump, SocialNetworkRelationshipType.HAS_SEEN);
        r6.setProperty("stars", 5);

        xiaoming.createRelationshipTo(xiaoqiang, SocialNetworkRelationshipType.FRIEND_OF);
        xiaoming.createRelationshipTo(xiaohong, SocialNetworkRelationshipType.FRIEND_OF);
        xiaoqiang.createRelationshipTo(xiaohong, SocialNetworkRelationshipType.FRIEND_OF);
    }

    public static void queryNodeOfSocialNetwork(GraphDatabaseService graphDb)
    {
        Node hitUser = null;
        for(Node node : graphDb.getAllNodes())
        {
          if(node.getProperty("name").equals("xiaoming"))
          {
            hitUser = node;
            break;
          }
        }

        Set<Node> recommendMoviesByFriends = new HashSet<>();

        for(Relationship r : hitUser.getRelationships(SocialNetworkRelationshipType.FRIEND_OF))
        {
          Node node = r.getOtherNode(hitUser);
          for(Relationship i : node.getRelationships(SocialNetworkRelationshipType.HAS_SEEN))
          {
            if(i.getProperty("stars").equals(5))
            {
              recommendMoviesByFriends.add(i.getEndNode());
            }
          }
        }

        Set<Node> recommendResult = new HashSet <>();
        for(Node movie:recommendMoviesByFriends)
        {
          boolean newMovie = true;
          for(Relationship j : movie.getRelationships(SocialNetworkRelationshipType.HAS_SEEN))
          {
            if(j.getStartNode().equals(hitUser))
            {
              newMovie = false;
            }
            if(newMovie)
            {
              recommendResult.add(movie);
              System.out.println(movie.getProperty("name"));
            }
          }
        }

        String cypherQuery = "start xiaoming = node(0) match (xiaoming)-[:FRIEND_OF]-()-[r:HAS_SEEN]-(movies) where not (xiaoming)-[:HAS_SEEN]-(movies) and r.stars=5 return movies";
        Map<String, Object> parameters = new HashMap<String, Object>();
        try(Result result = graphDb.execute(cypherQuery, parameters))
        {
          while(result.hasNext())
          {
            Map<String, Object> row = result.next();
            for(String key:result.columns())
            {
              System.out.printf("%s = %s%n", key, row.get(key));
            }
          }
        }

        // 定义遍历规则
        TraversalDescription traversalMoviesFriendsLike = graphDb.traversalDescription()
                .relationships(SocialNetworkRelationshipType.FRIEND_OF)
                .relationships(SocialNetworkRelationshipType.HAS_SEEN, Direction.OUTGOING)
                .uniqueness(Uniqueness.NODE_GLOBAL).evaluator(Evaluators.atDepth(2))
                .evaluator(new RecomandEvaluator(hitUser));

        Traverser traverser = traversalMoviesFriendsLike.traverse(hitUser);
        Iterable<Node> moviesRecomend = traverser.nodes();

        for (Node movie : moviesRecomend) {
            System.out. println("Recomend Movie: " + movie.getProperty("name"));
        }
    }

}
