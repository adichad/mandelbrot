package com.askme.mandelbrot.lucene.analysis.stem;

/**
 * Created by adichad on 01/05/15.
 */

import com.askme.mandelbrot.lucene.analysis.PayloadMasker;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.nio.CharBuffer;


/**
 * Created by adichad on 01/05/15.
 * from: https://sourceforge.net/p/lucense/code/HEAD/tree/lucense2/trunk/src/main/com/cleartrip/sw/search/analysis/filters/stem/ControlledPluralStemmer.java
 */
public class AugmentingStemmingTokenFilter extends TokenFilter {
    private final Stemmer stemmer;
    private final PayloadMasker masker;

    private final CharTermAttribute termAtt;
    private final PositionIncrementAttribute posAtt;

    private boolean writeStemSwitch = false;
    private CharBuffer buff;

    /**
     *
     * @param input parent tokenStream
     * @param stemmer the stemmer to use
     * @param markStem whether to mark successful stems in their payloads
     * @param payloadBitPosition payload bit position to mark stems (applicable only when <code>markStem = true</code>
     */
    public AugmentingStemmingTokenFilter(TokenStream input,
                                  boolean markStem, int payloadBitPosition, Stemmer stemmer) {
        super(input);
        this.stemmer = stemmer;
        this.termAtt = addAttribute(CharTermAttribute.class);
        this.posAtt = addAttribute(PositionIncrementAttribute.class);

        if(markStem)
            this.masker = new PayloadMasker.SingleBitMasker(payloadBitPosition).init(addAttribute(PayloadAttribute.class));
        else
            this.masker = PayloadMasker.noopMasker;

    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (this.writeStemSwitch) {
            this.posAtt.setPositionIncrement(0);
            this.termAtt.copyBuffer(this.buff.array(), 0, this.buff.length());
            this.writeStemSwitch = false;
            this.masker.mask();
            return true;
        }
        if (!this.input.incrementToken()) {
            this.input.end();
            return false;
        }

        if (this.stemmer.stem(this.termAtt.buffer(), 0, this.termAtt.length())) {
            this.buff = (CharBuffer.wrap(this.stemmer.getResultBuffer(), 0, this.stemmer.getResultLength()));
            this.writeStemSwitch = true;
        }
        return true;
    }
}