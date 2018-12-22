import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class RecomandEvaluator implements Evaluator
{
    private final Node userNode;

    public RecomandEvaluator(Node userNode)
    {
        this.userNode = userNode;
    }

    @Override
    public Evaluation evaluate(Path path)
    {
        Node currentNode = path.endNode();
        if (!currentNode.hasLabel(SocialNetworkLabel.MOVIE)) {
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }

        for (Relationship r : currentNode.getRelationships(Direction.INCOMING,
                SocialNetworkRelationshipType.HAS_SEEN)) {
            if (r.getStartNode().equals(userNode) || !r.getProperty("stars").equals(5)) {
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        }

        return Evaluation.INCLUDE_AND_CONTINUE;
    }
}
