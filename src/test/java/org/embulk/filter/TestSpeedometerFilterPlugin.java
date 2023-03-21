package org.embulk.filter;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.SpeedometerFilterPlugin.PluginTask;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.TaskMapper;
import org.junit.Test;

public class TestSpeedometerFilterPlugin
{
    @Mocked
    Exec exec;

    @Mocked
    ConfigSource config;

    @Mocked
    PluginTask task;

    @Mocked
    TaskSource taskSource;

    @Mocked
    Schema schema;

    @Mocked
    PageOutput inPageOutput;

    @Mocked
    FilterPlugin.Control control;

    @Mocked
    ConfigMapper configMapper;

    @Mocked
    TaskMapper taskMapper;

    @Test
    public void testTransaction() {
        new Expectations() {{
            configMapper.map(config, PluginTask.class); result = task; minTimes = 0;
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        plugin.transaction(config, schema, control);

        new Verifications() {{
            configMapper.map(config, PluginTask.class); times = 1;
            control.run((TaskSource)any, schema); times = 1;
        }};
    }

    @Test
    public void testOpen(final @Mocked PageReader reader, final @Mocked PageBuilder builder, final @Mocked Page page) throws Exception {
        new Expectations() {{
            taskMapper.map(taskSource, PluginTask.class); result = task; minTimes = 0;
            task.getDelimiter(); result = ""; minTimes = 0;
            reader.nextRecord(); result = true; result = false; minTimes = 0;
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.add(page);

        new Verifications() {{
            taskMapper.map(taskSource, PluginTask.class); times = 1;
            builder.addRecord(); times = 1;
            builder.finish(); times = 0;
            reader.nextRecord(); times = 2;
            reader.setPage(page); times = 1;
            schema.visitColumns(withInstanceOf(ColumnVisitor.class)); times = 1;
        }};
    }

    @Test
    public void testFinish(final @Mocked PageReader reader, final @Mocked PageBuilder builder, final @Mocked Page page) throws Exception {
        new Expectations() {{
            taskMapper.map(taskSource, PluginTask.class); result = task; minTimes = 0;
            task.getDelimiter(); result = ""; minTimes = 0;
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.finish();

        new Verifications() {{
            taskMapper.map(taskSource, PluginTask.class); times = 1;
            builder.finish(); times = 1;
        }};
    }

    @Test
    public void testClose(final @Mocked PageReader reader, final @Mocked PageBuilder builder, final @Mocked Page page) throws Exception {
        new Expectations() {{
            taskMapper.map(taskSource, PluginTask.class); result = task; minTimes = 0;
            task.getDelimiter(); result = ""; minTimes = 0;
        }};

        SpeedometerFilterPlugin plugin = new SpeedometerFilterPlugin();
        PageOutput output = plugin.open(taskSource, schema, schema, inPageOutput);
        output.close();

        new Verifications() {{
            taskMapper.map(taskSource, PluginTask.class); times = 1;
            builder.close(); times = 1;
        }};
    }
}
