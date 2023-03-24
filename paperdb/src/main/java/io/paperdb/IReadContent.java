package io.paperdb;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author lazyduck037
 */
public interface IReadContent {
    <E> E readContentRetry(File originalFile);

    <E> E readContent(File originalFile) throws FileNotFoundException;
}
