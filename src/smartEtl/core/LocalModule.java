package smartEtl.core;
/*
 * LocalModule.java
 *
 * Herbert Laroca, 2005
 */

public interface LocalModule {
    
    public String[] transform(MiniETL oetl, String strSeq);
           
} 