package com.askme.mandelbrot.lucene.analysis.stem;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by adichad on 01/05/15.
 * Porter stemming modified to stem only plurals, in a more controlled setting
 */
public class PluralStemmer extends Stemmer {
    private char[] b;

    private int i, /* offset into b */
            j, k, k0;

    private boolean dirty = false;

    private Set<String> blackList;

    private int minLen;

    private static final int INC = 50; /* unit of size whereby b is increased */

    private static final int EXTRA = 1;

    public PluralStemmer() {
        this.b = new char[INC];
        this.i = 0;
        this.blackList = new HashSet<String>();
        this.minLen = 1;
    }

    public PluralStemmer(Set<String> blackList) {
        this();
        if (blackList != null) {
            this.blackList = blackList;
        }
    }

    public PluralStemmer(Set<String> blackList, int minLen) {
        this(blackList);
        this.minLen = minLen;
    }

    public PluralStemmer(int minLen) {
        this();
        this.minLen = minLen;
    }

    /**
     * reset() resets the stemmer so it can stem another word. If you invoke the
     * stemmer by calling add(char) and then stem(), you must call reset() before
     * starting another word.
     */
    public void reset() {
        this.i = 0;
        this.dirty = false;
    }

    /**
     * Add a character to the word being stemmed. When you are finished adding
     * characters, you can call stem(void) to process the word.
     */
    public void add(char ch) {
        if (this.b.length <= this.i + EXTRA) {
            char[] new_b = new char[this.b.length + INC];
            System.arraycopy(this.b, 0, new_b, 0, this.b.length);
            this.b = new_b;
        }
        this.b[this.i++] = ch;
    }

    /**
     * After a word has been stemmed, it can be retrieved by toString(), or a
     * reference to the internal buffer can be retrieved by getResultBuffer and
     * getResultLength (which is generally more efficient.)
     */
    @Override
    public String toString() {
        return new String(this.b, 0, this.i);
    }

    /**
     * Returns the length of the word resulting from the stemming process.
     */
    @Override
    public int getResultLength() {
        return this.i;
    }

    /**
     * Returns a reference to a character buffer containing the results of the
     * stemming process. You also need to consult getResultLength() to determine
     * the length of the result.
     */
    @Override
    public char[] getResultBuffer() {
        return this.b;
    }

  /* cons(i) is true <=> b[i] is a consonant. */

    private final boolean cons(int i) {
        switch (this.b[i]) {
            case 'a':
            case 'e':
            case 'i':
            case 'o':
            case 'u':
                return false;
            case 'y':
                return (i == this.k0) ? true : !cons(i - 1);
            default:
                return true;
        }
    }

  /*
   * m() measures the number of consonant sequences between k0 and j. if c is a
   * consonant sequence and v a vowel sequence, and <..> indicates arbitrary
   * presence, <c><v> gives 0 <c>vc<v> gives 1 <c>vcvc<v> gives 2 <c>vcvcvc<v>
   * gives 3 ....
   */

    private final int m() {
        int n = 0;
        int i = this.k0;
        while (true) {
            if (i > this.j)
                return n;
            if (!cons(i))
                break;
            i++;
        }
        i++;
        while (true) {
            while (true) {
                if (i > this.j)
                    return n;
                if (cons(i))
                    break;
                i++;
            }
            i++;
            n++;
            while (true) {
                if (i > this.j)
                    return n;
                if (!cons(i))
                    break;
                i++;
            }
            i++;
        }
    }

  /*
   * cvc(i) is true <=> i-2,i-1,i has the form consonant - vowel - consonant and
   * also if the second c is not w,x or y. this is used when trying to restore
   * an e at the end of a short word. e.g. cav(e), lov(e), hop(e), crim(e), but
   * snow, box, tray.
   */

    private final boolean cvc(int i) {
        if ((i < this.k0 + 2) || !cons(i) || cons(i - 1) || !cons(i - 2))
            return false;
        else {
            int ch = this.b[i];
            if ((ch == 'w') || (ch == 'x') || (ch == 'y'))
                return false;
        }
        return true;
    }

    private final boolean ends(String s) {
        int l = s.length();
        int o = this.k - l + 1;
        if (o < this.k0)
            return false;
        for (int i = 0; i < l; i++)
            if (this.b[o + i] != s.charAt(i))
                return false;
        this.j = this.k - l;
        return true;
    }

  /*
   * setto(s) sets (j+1),...k to the characters in the string s, readjusting k.
   */

    void setto(String s) {
        int l = s.length();
        int o = this.j + 1;
        for (int i = 0; i < l; i++)
            this.b[o + i] = s.charAt(i);
        this.k = this.j + l;
        this.dirty = true;
    }

  /* r(s) is used further down. */

    void r(String s) {
        if (m() > 0)
            setto(s);
    }

  /*
   * step1() gets rid of plurals and -ed or -ing. e.g. caresses -> caress ponies
   * -> poni ties -> ti caress -> caress cats -> cat feed -> feed agreed ->
   * agree disabled -> disable matting -> mat mating -> mate meeting -> meet
   * milling -> mill messing -> mess meetings -> meet
   */

    private final boolean step1() {
        boolean flag = false;
        if (this.b[this.k] == 's') {
            if (ends("sses")) {
                this.k -= 2;
                flag = true;
            } else if (ends("ies"))
                setto("y"); // i
            else if ((this.b[this.k - 1] != 's') && (this.b[this.k - 1] != 'u') && (this.b[this.k - 1] != 'i')) {
                this.k--;
                flag = true;
            }
        }
        return flag;
    }

  /* step6() removes a final -e if m() > 1. */

    private final void step6() {
        this.j = this.k;
        if (this.b[this.k] == 'e') {
            int a = m();
            if (((a > 1) || ((a == 1) && !cvc(this.k - 1))) && !legitimateE(this.k - 1))
                this.k--;
        }
        // if (b[k] == 'l' && doublec(k) && m() > 1)
        // k--;
    }

    private boolean legitimateE(int i) {
        int c = this.b[i];

        if ((c == 'l') || (c == 'r') || (c == 'v') || (c == 'u') || (c == 'g') || (c == 'c') || (c == 't') || (c == 'n')
                || (c == 'm') || (c == 's') || (c == 'd') || (c == 'p') || (c == 'b') || (c == 'e') || (c == 'z'))
            return true; // ttle,
        if (c == 'h') {
            if (i > 0) {
                int d = this.b[i - 1];
                if (d == 't')
                    return true;
                else
                    return false;
            }
            return true;
        }
        if (c == 'k') {
            if (i > 0) {
                if (cons(i - 1))
                    return false;
                return true;
            }
            return true;
        }
        // if(!(doublec(c) && c=='s'))
        // return true;
        return false;
    }

    /**
     * Stem a word provided as a String. Returns the result as a String.
     */
    public String stem(String s) {
        if (stem(s.toCharArray(), s.length()))
            return toString();
        else
            return s;
    }

    /**
     * Stem a word contained in a char[]. Returns true if the stemming process
     * resulted in a word different from the input. You can retrieve the result
     * with getResultLength()/getResultBuffer() or toString().
     */
    public boolean stem(char[] word) {
        return stem(word, word.length);
    }

    /**
     * Stem a word contained in a portion of a char[] array. Returns true if the
     * stemming process resulted in a word different from the input. You can
     * retrieve the result with getResultLength()/getResultBuffer() or toString().
     */
    @Override
    public boolean stem(char[] wordBuffer, int offset, int wordLen) {
        reset();
        if (this.b.length < wordLen) {
            char[] new_b = new char[wordLen + EXTRA];
            this.b = new_b;
        }
        System.arraycopy(wordBuffer, offset, this.b, 0, wordLen);
        this.i = wordLen;
        return stem(0);
    }

    /**
     * Stem a word contained in a leading portion of a char[] array. Returns true
     * if the stemming process resulted in a word different from the input. You
     * can retrieve the result with getResultLength()/getResultBuffer() or
     * toString().
     */
    public boolean stem(char[] word, int wordLen) {
        return stem(word, 0, wordLen);
    }

    /**
     * Stem the word placed into the Stemmer buffer through calls to add().
     * Returns true if the stemming process resulted in a word different from the
     * input. You can retrieve the result with getResultLength()/getResultBuffer()
     * or toString().
     */
    public boolean stem() {
        return stem(0);
    }

    public boolean stem(int i0) {
        if (this.blackList.contains(new String(this.b, 0, this.i)) || (this.i < this.minLen))
            return false;

        this.k = this.i - 1;
        this.k0 = i0;
        if (this.k > this.k0 + 1) {
            if (step1())
                step6();
        }
        // Also, a word is considered dirty if we lopped off letters
        // Thanks to Ifigenia Vairelles for pointing this out.
        if (this.i != this.k + 1)
            this.dirty = true;
        this.i = this.k + 1;
        return this.dirty;
    }
}