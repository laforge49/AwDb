package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.ids.composites.Link1Id;
import org.agilewiki.awdb.db.immutable.collections.MapNode;

public class Link1Tran implements Transaction {
    @Override
    public void transform(Db db, MapNode tMapNode) {
        long timestamp = db.getTimestamp();
        String johnJonesId = NameId.generate("JohnJones");
        String jackJonesId = NameId.generate("JackJones");
        String sonId = NameId.generate("son");

        Link1Id.createLink1(db, johnJonesId, sonId, jackJonesId);
        for (String labelId: Link1Id.link1LabelIdIterable(db, johnJonesId)) {
            System.out.println("\nlink1 label: "+labelId);
            for (String targetId: Link1Id.link1IdIterable(db, johnJonesId, labelId, timestamp)) {
                System.out.println("target: "+targetId);
            }
        }
        for (String labelId: Link1Id.link1LabelInvIterable(db, jackJonesId)) {
            System.out.println("\nlink1Inv label: "+labelId);
            for (String targetId: Link1Id.link1InvIterable(db, jackJonesId, labelId, timestamp)) {
                System.out.println("originating: "+targetId);
            }
        }
        System.out.println("");
        for (String vmnId: Link1Id.label1IdIterable(db, sonId, db.getTimestamp())) {
            System.out.println("has a son link: "+vmnId);
        }
        System.out.println("");
        for (String vmnId: Link1Id.label1InvIterable(db, sonId, db.getTimestamp())) {
            System.out.println("has an inverted son link: "+vmnId);
        }

        Display.all(db, timestamp);

        Link1Id.removeLink1(db, johnJonesId, sonId, jackJonesId);
        for (String labelId: Link1Id.link1LabelIdIterable(db, johnJonesId)) {
            System.out.println("\nlink1 label: "+labelId);
            for (String targetId: Link1Id.link1IdIterable(db, johnJonesId, labelId, timestamp)) {
                System.out.println("target: "+targetId);
            }
        }
        for (String labelId: Link1Id.link1LabelInvIterable(db, jackJonesId)) {
            System.out.println("\nlink1Inv label: "+labelId);
            for (String targetId: Link1Id.link1InvIterable(db, jackJonesId, labelId, timestamp)) {
                System.out.println("originating: "+targetId);
            }
        }
        System.out.println("");
        for (String vmnId: Link1Id.label1IdIterable(db, sonId, db.getTimestamp())) {
            System.out.println("has a son link: "+vmnId);
        }
        System.out.println("");
        for (String vmnId: Link1Id.label1InvIterable(db, sonId, db.getTimestamp())) {
            System.out.println("has an inverted son link: "+vmnId);
        }

        Display.all(db, timestamp);
    }
}
