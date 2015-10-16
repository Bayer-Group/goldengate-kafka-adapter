package com.monsanto.data.goldengate;

import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.monsanto.data.goldengate.config.Conf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Tx.class, DsEvent.class, Op.class, DsTransaction.class, DsOperation.class, EventHandler.class, AbstractHandler.class})
public class EventHandler_UT {
    @Mock
    private MessageEncoder messageEncoder;
    @Mock
    private MessageProducer messageProducer;
    @Mock
    private DsEvent mockEvent;
    @Mock
    private Tx mockTx;
    @Mock
    private Op mockOp;
    @Mock
    private DsMetaData dsMetaData;
    @Mock
    private DsConfiguration dsConfiguration;
    @Mock
    private DsTransaction mockDsTx;
    @Mock
    private DsOperation mockDsOp;
    @Mock
    private Conf configuration;
    @Mock
    private Tx tx;
    @Mock
    private Op op;
    @Mock
    private TxFactory txFactory;

    private byte[] encodedBytes = new byte[1];

    private EventHandler eventHandler;

    @Before
    public void setUp() {
        eventHandler = spy(new EventHandler(configuration, messageEncoder, messageProducer, txFactory));
        eventHandler.setState(DataSourceListener.State.READY);
        when(eventHandler.isOperationMode()).thenReturn(false);

        when(txFactory.createAdapterTx(Matchers.any(DsTransaction.class), Matchers.any(DsMetaData.class),
                Matchers.any(DsConfiguration.class))).thenReturn(tx);

        when(tx.iterator()).thenReturn(Collections.singletonList(op).listIterator());
        when(messageEncoder.encode(tx, op)).thenReturn(encodedBytes);
    }

    @Test
    public void transactionCommit_publishesMessageForTransaction() {
        GGDataSource.Status status = eventHandler.transactionCommit(mockEvent, mockDsTx);

        assertThat(status, equalTo(GGDataSource.Status.OK));
        verify(messageProducer).produce(encodedBytes);
    }

    @Test
    public void transactionCommit_publishesMessagesForAllOpsInaTransaction() {
        Op op2 = mock(Op.class);
        when(tx.iterator()).thenReturn(Arrays.asList(op, op2).listIterator());

        byte[] secondOpByteArray = new byte[2];
        when(messageEncoder.encode(tx, op2)).thenReturn(secondOpByteArray);

        eventHandler.transactionCommit(mockEvent, mockDsTx);

        verify(messageProducer).produce(encodedBytes);
        verify(messageProducer).produce(secondOpByteArray);
    }

    @Test
    public void transactionCommit_returnsAbendWhenEncoderFails() {
        when(messageEncoder.encode(tx, op)).thenThrow(new RuntimeException());

        GGDataSource.Status status = eventHandler.transactionCommit(mockEvent, mockDsTx);

        assertThat(status, equalTo(GGDataSource.Status.ABEND));
    }

    @Test
    public void transactionCommit_returnsAbendWhenProducerFails() {
        doThrow(new RuntimeException()).when(messageProducer).produce(encodedBytes);

        GGDataSource.Status status = eventHandler.transactionCommit(mockEvent, mockDsTx);

        assertThat(status, equalTo(GGDataSource.Status.ABEND));
    }

}
