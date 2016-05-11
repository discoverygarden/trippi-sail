package org.trippi.impl.sesame;

import org.jrdf.graph.Triple;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.trippi.AliasManager;
import org.trippi.TrippiException;

public class ReadOnlySesameSession extends AbstractSesameSession {

	public ReadOnlySesameSession(Repository repository, AliasManager aliasManager, String serverUri, String model)
			throws RepositoryException {
		super(repository, aliasManager, serverUri, model);
		// TODO Auto-generated constructor stub
	}

	public ReadOnlySesameSession(Repository repository, String serverUri, String model) throws RepositoryException {
		super(repository, serverUri, model);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void doTriples(Iterable<Triple> triples, boolean add) throws TrippiException {
		throw new TrippiException("Read-only Sesame Session; cannot add or delete triples.");
	}
}
