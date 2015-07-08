package com.askme.mandelbrot.lucene.analysis.stem;

/**
 * Created by adichad on 01/05/15.
 * from: https://sourceforge.net/p/lucense/code/HEAD/tree/lucense2/trunk/src/main/com/cleartrip/sw/search/analysis/filters/stem/ControlledPluralStemmer.java
 */
import java.io.IOException;
import java.util.Set;

import com.askme.mandelbrot.lucene.analysis.PayloadMasker;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;

public class ReplacingStemmingTokenFilter extends TokenFilter {
    private final Stemmer stemmer;
    private final CharTermAttribute termAtt;
    private final PayloadMasker masker;

    /**
     *
     * @param input parent tokenStream
     * @param stemmer the stemmer to use
     * @param markStem whether to mark successful stems in their payloads
     * @param payloadBitPosition payload bit position to mark stems (applicable only when <code>markStem = true</code>
     */
    public ReplacingStemmingTokenFilter(TokenStream input,
                                 boolean markStem, int payloadBitPosition, Stemmer stemmer) {
        super(input);
        this.stemmer = stemmer;
        this.termAtt = addAttribute(CharTermAttribute.class);

        if(markStem)
            this.masker = new PayloadMasker.SingleBitMasker(payloadBitPosition).init(addAttribute(PayloadAttribute.class));
        else
            this.masker = PayloadMasker.noopMasker;

    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (!this.input.incrementToken()) {
            this.input.end();
            return false;
        }

        if (this.stemmer.stem(this.termAtt.buffer(), 0, this.termAtt.length())) {
            this.termAtt.copyBuffer(this.stemmer.getResultBuffer(), 0, this.stemmer.getResultLength());
            this.masker.mask();
        }
        return true;
    }


}