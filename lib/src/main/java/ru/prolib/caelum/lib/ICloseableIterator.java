package ru.prolib.caelum.lib;

import java.util.Iterator;

public interface ICloseableIterator<T> extends Iterator<T>, AutoCloseable {

}
