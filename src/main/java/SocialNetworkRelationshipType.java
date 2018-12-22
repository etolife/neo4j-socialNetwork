import org.neo4j.graphdb.RelationshipType;

public enum SocialNetworkRelationshipType implements RelationshipType
{
  FRIEND_OF,
  HAS_SEEN
}
