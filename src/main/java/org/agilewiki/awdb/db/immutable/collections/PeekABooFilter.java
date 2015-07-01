package org.agilewiki.awdb.db.immutable.collections;

/**
 * Iterates over selected values.
 */
abstract public class PeekABooFilter<T1> implements PeekABoo<T1> {
    private final PeekABoo<T1> peekABoo;

    public PeekABooFilter(PeekABoo<T1> peekABoo) {
        this.peekABoo = peekABoo;
        position();
    }

    abstract protected boolean select(T1 value);

    private void position() {
        while (peekABoo.hasNext() &&
                !select(peekABoo.peek()) &&
                peekABoo.hasNext()) {
            peekABoo.next();
        }
    }

    @Override
    public boolean positionPrior() {
        String position = getPosition();
        while (peekABoo.positionPrior()) {
            if (select(peekABoo.peek()))
                return true;
        }
        setPosition(position);
        return false;
    }

    @Override
    public String getPosition() {
        return peekABoo.getPosition();
    }

    @Override
    public void setPosition(String position) {
        peekABoo.setPosition(position);
        position();
    }

    @Override
    public T1 peek() {
        return peekABoo.peek();
    }

    @Override
    public boolean hasNext() {
        return peekABoo.hasNext();
    }

    @Override
    public T1 next() {
        T1 rv = peekABoo.next();
        position();
        return rv;
    }
}
