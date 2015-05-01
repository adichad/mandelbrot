package com.askme.mandelbrot.lucene.analysis.misc;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

/**
 * Created by adichad on 01/05/15.
 */
public class RecombiningTokenFilter extends TokenFilter {
    private final StringBuilder sb;
    private final CharTermAttribute termAttr;
    private final PositionIncrementAttribute posIncrAttr;
    private final String separator;

    public RecombiningTokenFilter(TokenStream input, String separator) {
        super(input);
        this.termAttr = addAttribute(CharTermAttribute.class);
        this.posIncrAttr = addAttribute(PositionIncrementAttribute.class);
        this.sb = new StringBuilder();
        this.separator = separator;
    }

    @Override
    public final void reset() throws IOException {
        super.reset();
        sb.setLength(0);
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if(input.incrementToken()) {
            this.sb.append(termAttr.buffer(), 0, termAttr.length());
            while (input.incrementToken()) {
                this.sb.append(separator).append(termAttr.buffer(), 0, termAttr.length());
            }
            this.termAttr.setEmpty().append(sb);
            this.posIncrAttr.setPositionIncrement(1);
            return true;
        }
        input.end();
        return false;
    }
}
