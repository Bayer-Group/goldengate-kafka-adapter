package com.monsanto.data.goldengate;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.google.common.annotations.VisibleForTesting;
import com.monsanto.data.goldengate.config.Conf;
import com.monsanto.data.goldengate.config.factory.ConfigurationFactory;
import com.monsanto.data.goldengate.config.factory.TypesafeConfigFactory;
import com.monsanto.data.goldengate.encoder.MessageEncoderFactory;
import com.monsanto.data.goldengate.kafka.KafkaProducerFactory;
import com.monsanto.data.goldengate.metrics.MetricsReporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.goldengate.atg.datasource.GGDataSource.Status;

public class EventHandler extends AbstractHandler {
    private static final Logger log = LoggerFactory.getLogger(EventHandler.class);

    private AtomicLong numOps = new AtomicLong(0);
    private AtomicLong numTxs = new AtomicLong(0);

    private ConfigurationFactory configurationFactory = new TypesafeConfigFactory();
    private Conf configuration;
    private MessageEncoder messageEncoder;
    private MessageProducer messageProducer;

    private MetricRegistry metrics = new MetricRegistry();
    private Timer operationProcessingTimer = metrics.timer("operationProcessingTime");
    private Timer messageEncodingTimer = metrics.timer("encodingTime");
    private Timer messageSendingTimer = metrics.timer("sendingTime");
    private Meter operationProcessingErrorMeter = metrics.meter("processingErrors");
    private TxFactory txFactory;
    private ScheduledReporter metricsReporter;

    private String configurationPath;

    public EventHandler() {
        super(TxOpMode.op);
        log.info("created handler - default mode: " + getMode());
    }

    @VisibleForTesting
    public EventHandler(Conf configuration, MessageEncoder messageEncoder, MessageProducer messageProducer, TxFactory txFactory) {
        this.configuration = configuration;
        this.messageEncoder = messageEncoder;
        this.messageProducer = messageProducer;
        this.txFactory = txFactory;
    }

    @Override
    public void init(DsConfiguration conf, DsMetaData metaData) {
        super.init(conf, metaData);
        log.info("Initializing handler: Mode =" + getMode());
        configuration = configurationFactory.load(configurationPath);
        messageProducer = KafkaProducerFactory.create(configuration.kafka());
        messageEncoder = MessageEncoderFactory.create(configuration);
        txFactory = new TxFactory();

        if (configuration.metrics().isEnabled()) {
            metricsReporter = MetricsReporterFactory.createReporter(configuration.metrics(), metrics);
        }
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction transaction) {
        super.transactionBegin(e, transaction);

        if (log.isDebugEnabled()) {
            log.debug("Received begin tx event, numTx="
                    + numTxs.get()
                    + " : position="
                    + transaction.getTranID()
                    + ", totalOps="
                    + transaction.getTotalOps());
        }

        return Status.OK;
    }

    @Override
    public Status operationAdded(DsEvent e, DsTransaction transaction, DsOperation operation) {
        Status overallStatus = Status.OK;
        super.operationAdded(e, transaction, operation);
        numOps.incrementAndGet();

        final Tx tx = new Tx(transaction, getMetaData(), getConfig());
        final TableMetaData tMeta = getMetaData().getTableMetaData(operation.getTableName());
        final Op op = new Op(operation, tMeta, getConfig());

        operation.getTokens();
        if (isOperationMode()) {

            if (log.isDebugEnabled()) {
                log.debug(" Received operation: table='"
                        + op.getTableName() + "'"
                        + ", pos=" + op.getPosition()
                        + " (total_ops= " + tx.getTotalOps()
                        + ", buffered=" + tx.getSize() + ")"
                        + ", ts=" + op.getTimestamp());

            }

            Status operationStatus = processOperation(tx, op);

            if (Status.ABEND.equals(operationStatus)) {
                overallStatus = Status.ABEND;
            }
        }
        return overallStatus;
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction transaction) {
        Status overallStatus = Status.OK;
        super.transactionCommit(e, transaction);

        Tx tx = txFactory.createAdapterTx(transaction, getMetaData(), getConfig());
        numTxs.incrementAndGet();

        if (log.isDebugEnabled()) {
            log.debug("transactionCommit event, tx #" + numTxs.get() + ":"
                    + ", pos=" + tx.getTranID()
                    + " (total_ops= " + tx.getTotalOps()
                    + ", buffered=" + tx.getSize() + ")"
                    + ", ts=" + tx.getTimestamp() + ")");
        }

        if (!isOperationMode()) {
            for (Op op : tx) {
                Status operationStatus = processOperation(tx, op);

                if (Status.ABEND.equals(operationStatus)) {
                    overallStatus = Status.ABEND;
                }
            }
        }

        return overallStatus;
    }

    private Status processOperation(Tx tx, Op op) {
        Timer.Context timer = operationProcessingTimer.time();
        Status status = Status.OK;

        try {
            encodeAndSend(tx,op);
        } catch (RuntimeException re) {
            operationProcessingErrorMeter.mark();
            log.error("Error processing operation: " + op.toString(), re);
            status = Status.ABEND;
        }

        timer.stop();
        return status;
    }

    @Override
    public Status metaDataChanged(DsEvent e, DsMetaData meta) {
        log.debug("Received metadata event: " + e
                + "; current tables: "
                + meta.getTableNames().size());
        return super.metaDataChanged(e, meta);
    }

    @Override
    public String reportStatus() {
        String s = "Status report: "
                + ", mode=" + getMode()
                + ", transactions=" + numTxs.get() + ", operations=" + numOps.get();
        return s;
    }

    @Override
    public void destroy() {
        log.debug("destroy()... " + reportStatus());
        if (configuration.metricsEnabled()) {
            metricsReporter.stop();
        }

        messageProducer.terminate();
        super.destroy();
    }

    private void encodeAndSend(Tx tx, Op op) {
        if (log.isDebugEnabled()) {
            log.debug("Processing of transaction " + tx + " and operation " + op);
        }

        byte[] encodedMessage = encodeMessage(tx, op);
        sendMessage(encodedMessage);

        if (log.isDebugEnabled()) {
            log.debug("Completed processing of transaction " + tx + " and operation " + op);
        }
    }

    private void sendMessage(byte[] encodedMessage)  {
        Timer.Context sendMessageTimer = messageSendingTimer.time();
        messageProducer.produce(encodedMessage);
        sendMessageTimer.stop();

        if (log.isDebugEnabled()) {
            log.debug("Completed send of message: " + new String(encodedMessage));
        }
    }

    private byte[] encodeMessage(Tx tx, Op op) {
        Timer.Context encodingTimer = messageEncodingTimer.time();
        byte[] encodedMessage = messageEncoder.encode(tx, op);
        encodingTimer.stop();

        if (log.isTraceEnabled()) {
            log.trace("Result of message encoding is = " + new String(encodedMessage));
        }

        return encodedMessage;
    }

    public void setConfigurationPath(String configurationPath) {
        this.configurationPath = configurationPath;
    }
}
