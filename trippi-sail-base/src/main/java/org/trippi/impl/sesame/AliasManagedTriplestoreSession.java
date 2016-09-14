package org.trippi.impl.sesame;

import org.trippi.AliasManager;
import org.trippi.impl.base.TriplestoreSession;

public interface AliasManagedTriplestoreSession extends TriplestoreSession {
    public AliasManager getAliasManager();
}
