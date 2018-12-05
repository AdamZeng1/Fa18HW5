package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class LockUtil {
    /**
     * Ensure that TRANSACTION can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     *
     * If TRANSACTION is null, this method should do nothing.
     */
    public static void requestLocks(BaseTransaction transaction, LockContext lockContext,
                                    LockType lockType) {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        if (transaction != null) {
            //check if lockContext is root
            boolean root = false;
            if (lockContext.parentContext() == null) {
                root = true;
            }

            //acquire a lock with lockType
            if (!root) {
                LockContext parentIter = lockContext.parentContext();
                List<LockContext> lockContextList = new ArrayList<>();
                lockContextList.add(lockContext);
                while (parentIter != null) {
                    lockContextList.add(parentIter);
                    parentIter = parentIter.parentContext();
                }
                System.out.print(lockContextList+"\n");
                if (lockContext.numChildLocks.get(transaction.getTransNum()) == null) {
                    for (int i = lockContextList.size() - 1; i >= 0; i--) {
                        //simple acquire and release
                        //acquire
                        if (lockContextList.get(i).getLocalLockType(transaction) == null) {
                            //parents
                            if (!lockContextList.get(i).equals(lockContext)) {
                                lockContextList.get(i).acquire(transaction, LockType.parentLock(lockType));
                            } else {
                                //this
                                lockContextList.get(i).acquire(transaction, lockType);
                            }
                        } else {
                            //promote
                            //parent
                            if (!lockContextList.get(i).equals(lockContext)) {
                                lockContextList.get(i).promote(transaction, LockType.parentLock(lockType));
                            } else {
                                //this
                                lockContextList.get(i).promote(transaction, lockType);
                            }
                        }
                    }
                } else {
                    System.out.print("escalate\n");
                    lockContext.escalate(transaction);
                }
            }
        }
    }

    // TODO(hw5): add helper methods as you see fit
}
