package ru.prolib.caelum.service;

public interface IExtension {
	default ExtensionStatus getStatus() { throw new UnsupportedOperationException(); }
	default void clear() { }
}
