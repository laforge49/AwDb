package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.ids.composites.Link2Id;
import org.agilewiki.awdb.db.immutable.collections.MapNode;

public class Link2Tran implements Transaction {
    @Override
    public void transform(Db db, MapNode tMapNode) {
        long timestamp = db.getTimestamp();
        String johnJonesId = NameId.generate("JohnJones");
        String jackJonesId = NameId.generate("JackJones");
        String brotherId = NameId.generate("brother");

        Link2Id.createLink2(db, johnJonesId, brotherId, jackJonesId);
        for (String labelId: Link2Id.link2LabelIdIterable(db, johnJonesId)) {
            System.out.println("\nlink2 label: "+labelId);
            for (String targetId: Link2Id.link2IdIterable(db, johnJonesId, labelId, timestamp)) {
                System.out.println("target: "+targetId);
            }
        }
        System.out.println("");
        for (String vmnId: Link2Id.label2IdIterable(db, brotherId, db.getTimestamp())) {
            System.out.println("has brother link: "+vmnId);
        }

        Display.all(db, timestamp);

        Link2Id.removeLink2(db, johnJonesId, brotherId, jackJonesId);
        for (String labelId: Link2Id.link2LabelIdIterable(db, johnJonesId)) {
            System.out.println("\nlink2 label: "+labelId);
            for (String targetId: Link2Id.link2IdIterable(db, johnJonesId, labelId, timestamp)) {
                System.out.println("target: "+targetId);
            }
        }
        System.out.println("");
        for (String vmnId: Link2Id.label2IdIterable(db, brotherId, db.getTimestamp())) {
            System.out.println("has brother link: " + vmnId);
        }

        Display.all(db, timestamp);
    }
}
