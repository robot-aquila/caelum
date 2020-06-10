package ru.prolib.caelum.core;

import java.util.Iterator;

public interface ICloseableIterator<T> extends Iterator<T>, AutoCloseable {

}
