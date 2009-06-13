"""Utilities for dealing with database transactions."""
import contextlib

from django.db import transaction as dj_transaction

@contextlib.contextmanager
def transaction():
    """Context manager for executing something in the context of a database
       transaction. A transaction is started and the nested block executed. If
       no exceptions are raised, the transaction is committed. Otherwise, the
       transaction is rolled back and the exception re-raised."""
    try:
        dj_transaction.enter_transaction_management()
        dj_transaction.managed(True)
        try:
            yield
        except:
            # The nested block threw an exception, roll back
            if dj_transaction.is_dirty():
                dj_transaction.rollback()
            raise
        else:
            # The nested block succeeded, commit
            if dj_transaction.is_dirty():
                dj_transaction.commit()
    finally:
        # Must always leave transaction management, even in the case
        # of an exception
        dj_transaction.leave_transaction_management()
