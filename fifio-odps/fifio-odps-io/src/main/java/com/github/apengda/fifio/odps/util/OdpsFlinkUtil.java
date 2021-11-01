package com.github.apengda.fifio.odps.util;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsReadOptions;
import com.github.apengda.fifio.odps.source.OdpsInputSplit;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OdpsFlinkUtil {

    /**
     * 读数据时 任务切分
     *
     * @param odps
     * @param options
     * @param readOptions
     * @return
     */
    public static OdpsInputSplit[] split(final Odps odps,
                                         final OdpsOptions options,
                                         final OdpsReadOptions readOptions) throws IOException {
        int splitIndex = 0;
        final List<OdpsInputSplit> splits = new ArrayList<>();
        if (readOptions.getQueryTemplate() != null) {
            OdpsInputSplit sp = new OdpsInputSplit(null, splitIndex++, options.getPartition(),
                    null, null, readOptions.getQueryTemplate());
            splits.add(sp);
        } else {
            final TableTunnel tableTunnel = new TableTunnel(odps);
            final Table table = OdpsUtil.getTable(odps, options.getProject(), options.getTable());
            final List<String> partitions = new ArrayList<>();

            if (options.getPartition() == null) {
                // 未指定分区
                if (OdpsUtil.isPartitioned(table)) {
                    List<String> ps = OdpsUtil.getPartitionsString(table);
                    partitions.addAll(ps);
                } else {

                }
            } else {
                partitions.add(options.getPartition());
            }

            if (partitions.size() == 0) {
                partitions.add("");
            }

            try {
                if (partitions.size() > 0) {
                    // 按分区 和 行数 进行任务切分
                    for (String p : partitions) {
                        if (StringUtils.isEmpty(p)) {
                            p = null;
                        }
                        List<OdpsInputSplit> sp = doSplit(splitIndex, tableTunnel, options, readOptions, p);
                        splitIndex += sp.size();
                        splits.addAll(sp);
                    }
                }
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }
        Collections.reverse(splits);
        return splits.toArray(new OdpsInputSplit[0]);
    }

    private static List<OdpsInputSplit> doSplit(int splitIndex, final TableTunnel tableTunnel,
                                                final OdpsOptions options, final OdpsReadOptions readOptions, String partition) throws TunnelException {
        final List<OdpsInputSplit> splits = new ArrayList<>();
        TableTunnel.DownloadSession session = createDownloadSession(tableTunnel, options, partition, null);
        long count = session.getRecordCount();
        if (count < 1) {
            return splits;
        }
        if (readOptions.getLimit() != null && readOptions.getLimit() > 0
                && count > readOptions.getLimit()) {
            count = readOptions.getLimit();
        }
        if (readOptions.getReadSplitStepSize() != null
                && readOptions.getReadSplitStepSize() >= 10000) {
            final Integer step = readOptions.getReadSplitStepSize();
            for (long i = 0; i < count; i += step) {
                long thisStep = step;
                if (i + step > count) {
                    thisStep = count - i;
                }
                OdpsInputSplit split = new OdpsInputSplit(session.getId(), splitIndex++, partition, i, thisStep, null);
                splits.add(split);
            }
        } else {
            OdpsInputSplit split = new OdpsInputSplit(session.getId(), splitIndex++, partition, 0l, count, null);
            splits.add(split);
        }
        return splits;
    }

    public static TableTunnel.DownloadSession createDownloadSession(TableTunnel tableTunnel, OdpsOptions options,
                                                                    String partition, String sessionId) throws TunnelException {
        TableTunnel.DownloadSession downloadSession;
        if (partition == null) {
            if (sessionId == null) {
                downloadSession = tableTunnel.createDownloadSession(options.getProject(), options.getTable());
            } else {
                downloadSession = tableTunnel.getDownloadSession(options.getProject(), options.getTable(), sessionId);
            }
        } else {
            PartitionSpec partitionSpec = new PartitionSpec(partition);
            if (sessionId == null) {
                downloadSession = tableTunnel.createDownloadSession(options.getProject(),
                        options.getTable(), partitionSpec);
            } else {
                downloadSession = tableTunnel.getDownloadSession(options.getProject(),
                        options.getTable(), partitionSpec, sessionId);
            }
        }
        return downloadSession;
    }
}
