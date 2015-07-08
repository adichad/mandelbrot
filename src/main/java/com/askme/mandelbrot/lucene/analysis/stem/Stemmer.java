package com.askme.mandelbrot.lucene.analysis.stem;

/**
 * Created by adichad on 01/05/15.
 */
public abstract class Stemmer {
    public abstract boolean stem(char[] wordBuffer, int offset, int wordLen);
    public abstract char[] getResultBuffer();
    public abstract int getResultLength();
}
