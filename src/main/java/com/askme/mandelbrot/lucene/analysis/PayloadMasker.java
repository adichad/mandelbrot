package com.askme.mandelbrot.lucene.analysis;

import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

/**
 * Created by adichad on 01/05/15.
 */
public abstract class PayloadMasker {
    public abstract void mask();

    public static final PayloadMasker noopMasker = new PayloadMasker() {
        @Override
        public void mask(){}
    };

    public static final class SingleBitMasker extends PayloadMasker {
        private final int payloadByteIndex;
        private final int payloadBitPosition;
        private final int payloadBytesMinSize;

        private PayloadAttribute payAtt;
        public SingleBitMasker(int payloadBitPosition) {
            this.payloadByteIndex = payloadBitPosition >> 3;
            this.payloadBitPosition = (byte)(payloadBitPosition % 8);
            this.payloadBytesMinSize = payloadByteIndex + 1;
        }

        public PayloadMasker init(PayloadAttribute payAtt) {
            this.payAtt = payAtt;
            return this;
        }

        @Override
        public void mask() {
            BytesRef payload = this.payAtt.getPayload();
            if (payload == null) {
                payload = new BytesRef(new byte[payloadBytesMinSize]);
            }
            byte[] b = payload.bytes;
            if (b.length < payloadBytesMinSize) {
                byte[] n = new byte[payloadBytesMinSize];
                System.arraycopy(payload.bytes, 0, n, 0, b.length);
                b = n;
            }

            b[payloadByteIndex] |= (1 << payloadBitPosition);
            this.payAtt.setPayload(payload);
        }

    }

}
