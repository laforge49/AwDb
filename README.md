# AwDb
A multi-metaphor database.

The AgileWiki Database is a multi-metaphor versioning repository.

The database holds a set of objects, each with its own unique identifier.
Objects have attributes.

Objects also represent rows in a table.
The object identifier is the primary key,
but there are also secondary keys implementing multiple indexes.
And joins are also supported.

Objects are also the nodes in a graph, with links connecting them.
These links have labels and there are indexes for accessing
the origin and destination nodes for a given label.

Every transaction creates a journal entry, which is also an object in the database.
Journal entries are connected to the objects, secondary keys and links they updated.
And it is a simple matter to view a journal for each of these
by displaying the journal entries which modified them.

Navigation through past time is also supported.
When doing so, the attributes, key values and links of an object
reflect the values they had at that past time.

The database is implemented using immutable structures,
making it easy to share data across threads
and to perform multiple simultaneous queries.