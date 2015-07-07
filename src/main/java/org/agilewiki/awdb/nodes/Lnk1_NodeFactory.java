package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.Node;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;

public class Lnk1_NodeFactory extends Metadata_NodeFactory {
    public final static String ID = "$nlnk1.node";

    final public static String TARGET_ID = "$ntarget";
    final public static String TARGET_LNK1_ID = "$ntarget.lnk1";

    final public static String ATTRIBUTEOF_ID = "$nattributeOf";
    final public static String ATTRIBUTEOF_LNK1_ID = "$nattributeOf.lnk1";
    final public static String ORIGIN_ID = "$norigin";
    final public static String ORIGIN_LNK1_ID = "$norigin.lnk1";
    final public static String DESTINATION_ID = "$ndestination";
    final public static String DESTINATION_LNK1_ID = "$ndestination.lnk1";
    final public static String USER_ID = "$nuser";
    final public static String USER_LNK1_ID = "$nuser.lnk1";
    final public static String USER_GROUP_ID = "$nuserGroup";
    final public static String USER_GROUP_LNK1_ID = "$nuserGroup.lnk1";
    final public static String USER_DOMAIN_ID = "$nuserDomain";
    final public static String USER_DOMAIN_LNK1_ID = "$nuserDomain.lnk1";
    final public static String MEMBER_OF_ID = "$nmemberOf";
    final public static String MEMBER_OF_LNK1_ID = "$nmemberOf.lnk1";
    final public static String ROLE_OF_REALM_ID = "$nroleOfRealm";
    final public static String ROLE_OF_REALM_LNK1_ID = "$nroleOfRealm.lnk1";
    final public static String GROUP_DOMAIN_ID = "$ngroupDomain";
    final public static String GROUP_DOMAIN_LNK1_ID = "$ngroupDomain.lnk1";
    final public static String DOMAIN_FOR_REALM_ID = "$ndomainForRealm";
    final public static String DOMAIN_FOR_REALM_LNK1_ID = "$ndomainForRealm.lnk1";
    final public static String DOMAIN_FOR_ROLE_ID = "$ndomainForRole";
    final public static String DOMAIN_FOR_ROLE_LNK1_ID = "$ndomainForRole.lnk1";
    final public static String OF_DOMAIN_ID = "$nofDomain";
    final public static String OF_DOMAIN_LNK1_ID = "$nofDomain.lnk1";

    public static void create(AwDb awDb) {
        awDb.addTimelessNode(new Lnk1_NodeFactory(ID, FactoryRegistry.MAX_TIMESTAMP));
    }

    public static void define(String nodeId, String invDependency, String originType, String destinationType) {
        AwDb awDb = getAwDb();
        Node_NodeFactory.define(nodeId, Lnk1_NodeFactory.ID, null);
        if (invDependency != null) {
            awDb.createSecondaryId(nodeId, Key_NodeFactory.INVDEPENDENCY_ID, invDependency);
        }
        awDb.createLnk1(nodeId,
                Lnk1_NodeFactory.ORIGIN_ID,
                originType);
        awDb.createLnk1(nodeId,
                Lnk1_NodeFactory.DESTINATION_ID,
                destinationType);
    }

    public Lnk1_NodeFactory(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    @Override
    public Node instantiateNode(String nodeId, long timestamp) {
        return new Lnk1_Node(nodeId, timestamp);
    }
}
