package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 */
public class LockManager {
    // These members are given as a suggestion. You are not required to use them, and may
    // delete them and add members as you see fit.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    private Map<ResourceName, List<Pair<Long, Lock>>> resourceLocks = new HashMap<>();
    private Deque<LockRequest> waitingQueue = new ArrayDeque<>();

    // You should not modify this.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    public LockManager() {}

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public LockContext databaseContext() {
        if (!contexts.containsKey("database")) {
            contexts.put("database", new LockContext(this, null, "database"));
        }
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock. No error checking is performed for holding
     * requisite parent locks or freeing dependent child locks. Blocks the transaction and
     * places it in queue if the requested lock is not compatible with another transaction's
     * lock on the resource. Unblocks and unqueues all transactions that can be unblocked
     * after releasing locks in RELEASELOCKS, in order of lock request.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        //TODO needs more work on acuquiring
        Lock lockOnName = new Lock(name, lockType);
        //Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION
        if (this.transactionLocks.containsKey(transaction.getTransNum())) {
            this.transactionLocks.get(transaction.getTransNum()).add(lockOnName);
        } else {
            List<Lock> list = new ArrayList<>();
            list.add(lockOnName);
            this.transactionLocks.put(transaction.getTransNum(), list);
        }

        //Releases all locks in RELEASELOCKS after acquiring the lock
        for (int i = 0; i < releaseLocks.size(); i++) {
            this.resourceLocks.remove(releaseLocks.get(i));
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION. No error
     * checking is performed for holding requisite parent locks. Blocks the
     * transaction and places it in queue if the requested lock is not compatible
     * with another transaction's lock on the resource.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        Lock lockOnName = new Lock(name, lockType);
        boolean conflict = false;
        Long sameTransactionLong = null;
        //check if the requested lock is not compatible with another transaction's lock on the resource
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            if (transaction.getTransNum() == key) {
                sameTransactionLong = key;
            } else {
                for (int i = 0; i < value.size(); i++) {
                    if (name == value.get(i).name) {
                        if (!LockType.compatible(lockType, value.get(i).lockType)) {
                            conflict = true;
                        }
                    }
                }
            }
        }

        //Acquire a LOCKTYPE lock on NAME, for transaction TRANSATION
        if (!conflict) {
            if (sameTransactionLong != null) {
                throw new DuplicateLockRequestException("a lock on NAME is held by TRANSACTION");
            } else {
                System.out.println(1); //debug
                List<Lock> list = new ArrayList<>();
                list.add(lockOnName);
                this.transactionLocks.put(transaction.getTransNum(), list);
            }
        } else {
            System.out.println(2); //debug
            //Add to queue
            LockRequest newRequest = new LockRequest(transaction, lockOnName);
            this.waitingQueue.add(newRequest);
        }

        /*if (this.resourceLocks.containsKey(name)) {
            this.resourceLocks.get(name).add(new Pair<>(transaction.getTransNum(), lockOnName));
        } else {
            List<Pair<Long, Lock>> resourcelist = new ArrayList<>();
            resourcelist.add(new Pair<>(transaction.getTransNum(), lockOnName));
            this.resourceLocks.put(name, resourcelist);
        }*/
    }

    /**
     * Release TRANSACTION's lock on NAME. No error checking is performed for
     * freeing dependent child locks. Unblocks and unqueues all transactions
     * that can be unblocked, in order of lock request.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        throw new UnsupportedOperationException("TODO(hw5): implement");
        //if (this.transactionLocks)
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE. No error checking is
     * performed for holding requisite locks. Blocks the transaction and places
     * TRANSACTION in the **front** of the queue if the request cannot be
     * immediately granted (i.e. another transaction holds a conflicting lock). A
     * lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }

    /**
     * Return the type of lock TRANSACTION has on NAME, or null if no lock is
     * held.
     */
    public LockType getLockType(BaseTransaction transaction, ResourceName name) {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        if (this.transactionLocks.containsKey(transaction.getTransNum())) {
            System.out.println("Contains key"); //debug
            List<Lock> locks = this.transactionLocks.get(transaction.getTransNum());
            for (int i = 0; i < locks.size(); i++) {
                if (name == locks.get(i).name) {
                    System.out.println(locks.get(i).lockType.toString()); //debug
                    return locks.get(i).lockType;
                }
            }
        } else {
            return null;
        }
        return null;
    }

    /**
     * Returns the list of transactions ids and lock types for locks held on
     * NAME, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<Long, LockType>> getLocks(ResourceName name) {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        List<Pair<Long, LockType>> list = new ArrayList<>();
//        for (int i = 0; i < this.resourceLocks.get(name).size(); i++) {
//            list.add(new Pair<>(this.resourceLocks.get(name).get(i).getFirst(), this.resourceLocks.get(name).get(i).getSecond().lockType));
//        }
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            for (int i = 0; i < value.size(); i++) {
                if (name == value.get(i).name) {
                    list.add(new Pair<>(key, value.get(i).lockType));
                }
            }
        }
        return list;
    }

    /**
     * Returns the list of resource names and lock types for locks held by
     * TRANSACTION, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<ResourceName, LockType>> getLocks(BaseTransaction transaction) {
        throw new UnsupportedOperationException("TODO(hw5): implement");
    }
}
