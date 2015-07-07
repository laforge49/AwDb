package org.agilewiki.awdb.nodes;

import org.agilewiki.awdb.AwDb;
import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.immutable.collections.MapNode;
import org.agilewiki.awdb.db.virtualcow.Db;

public class InitializeDatabase_Node extends JournalEntry_Node {
    public final static String NAME = "initializeDatabase";

    public InitializeDatabase_Node() {
    }

    public InitializeDatabase_Node(String nodeId, long timestamp) {
        super(nodeId, timestamp);
    }

    public static String update(AwDb awDb)
            throws Exception {
        MapNode mn = awDb.nilMap;
        return awDb.update(NAME, mn).call();
    }

    @Override
    public void process(Db db, MapNode tMapNode) {

        Node_NodeFactory.define(Delete_NodeFactory.ID, Node_NodeFactory.ID, JournalEntry_NodeFactory.ID,
                NameId.USER_KEY, NameId.AN_ID);
        Node_NodeFactory.define(Metadata_NodeFactory.ID, Node_NodeFactory.ID, null);
        Node_NodeFactory.define(Node_NodeFactory.ID, Node_NodeFactory.ID, Metadata_NodeFactory.ID);
        Node_NodeFactory.define(Key_NodeFactory.ID, Node_NodeFactory.ID, Metadata_NodeFactory.ID);
        Node_NodeFactory.define(Lnk1_NodeFactory.ID, Node_NodeFactory.ID, Metadata_NodeFactory.ID);
        Node_NodeFactory.define(Attribute_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID,
                NameId.SUBJECT);
        Node_NodeFactory.define(JournalEntry_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID);
        Node_NodeFactory.define(User_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID,
                NameId.SUBJECT);
        Node_NodeFactory.define(User_NodeFactory.SYSTEM_USER_ID, User_NodeFactory.ID, null);
        Node_NodeFactory.define(Realm_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID);
        Node_NodeFactory.define(Realm_NodeFactory.SYSTEM_REALM_ID, Realm_NodeFactory.ID, null);
        Node_NodeFactory.define(Realm_NodeFactory.USER_REALM_ID, Realm_NodeFactory.ID, null);
        Node_NodeFactory.define(Group_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID);
        Node_NodeFactory.define(Group_NodeFactory.USERS_GROUP_ID, Group_NodeFactory.ID, null);
        Node_NodeFactory.define(Group_NodeFactory.ADMINS_GROUP_ID, Group_NodeFactory.ID, null);
        Node_NodeFactory.define(Domain_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID);
        Node_NodeFactory.define(Role_NodeFactory.ID, Node_NodeFactory.ID, Node_NodeFactory.ID);

        Key_NodeFactory.define(Key_NodeFactory.NODETYPE_KEY_ID, Node_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.SUPERTYPE_KEY_ID, Node_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.ATTRIBUTENAME_KEY_ID, Attribute_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.INVDEPENDENCY_KEY_ID, Lnk1_NodeFactory.ID);
        Key_NodeFactory.define(Key_NodeFactory.SUBJECT_KEY_ID, Node_NodeFactory.ID);

        Lnk1_NodeFactory.define(Lnk1_NodeFactory.TARGET_LNK1_ID, null, Key_NodeFactory.ID, Node_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.ATTRIBUTEOF_LNK1_ID, Lnk1_NodeFactory.ATTRIBUTEOF_ID, Attribute_NodeFactory.ID, Metadata_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.ORIGIN_LNK1_ID, null, Lnk1_NodeFactory.ID, Node_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.DESTINATION_LNK1_ID, null, Lnk1_NodeFactory.ID, Node_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.USER_LNK1_ID, null, Metadata_NodeFactory.ID, User_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.USER_GROUP_LNK1_ID, null, User_NodeFactory.ID, Group_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.MEMBER_OF_LNK1_ID, null, User_NodeFactory.ID, Group_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.USER_DOMAIN_LNK1_ID, null, User_NodeFactory.ID, Domain_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.ROLE_OF_REALM_LNK1_ID, null, Role_NodeFactory.ID, Realm_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.GROUP_DOMAIN_LNK1_ID, null, Group_NodeFactory.ID, Domain_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.DOMAIN_FOR_REALM_LNK1_ID, null, Domain_NodeFactory.ID, Realm_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.DOMAIN_FOR_ROLE_LNK1_ID, null, Domain_NodeFactory.ID, Role_NodeFactory.ID);
        Lnk1_NodeFactory.define(Lnk1_NodeFactory.OF_DOMAIN_LNK1_ID, null, Node_NodeFactory.ID, Domain_NodeFactory.ID);

        getAwDb().createLnk1(User_NodeFactory.SYSTEM_USER_ID, Lnk1_NodeFactory.USER_GROUP_ID, Group_NodeFactory.USERS_GROUP_ID);
        getAwDb().createLnk1(User_NodeFactory.SYSTEM_USER_ID, Lnk1_NodeFactory.USER_GROUP_ID, Group_NodeFactory.ADMINS_GROUP_ID);
    }
}
