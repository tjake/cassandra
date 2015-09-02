package org.apache.cassandra.cql3;

import java.util.Collections;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.functions.Function;

/**
 * Base class for <code>Conditions</code> classes.
 *
 */
abstract class AbstractConditions implements Conditions
{
    @Override
    public Iterable<Function> getFunctions()
    {
        return Collections.emptyList();
    }

    @Override
    public Iterable<ColumnDefinition> getColumns()
    {
        return null;
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public boolean applyToStaticColumns()
    {
        return false;
    }

    @Override
    public boolean applyToRegularColumns()
    {
        return false;
    }

    @Override
    public boolean isIfExists()
    {
        return false;
    }

    @Override
    public boolean isIfNotExists()
    {
        return false;
    }
}
