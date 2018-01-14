//yy
import org.omg.CORBA.IntHolder;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.*;
import org.omg.CosNaming.NamingContextPackage.CannotProceed;
import org.omg.CosNaming.NamingContextPackage.InvalidName;
import org.omg.CosNaming.NamingContextPackage.NotFound;
import org.omg.PortableServer.POA;
import org.omg.PortableServer.POAManagerPackage.AdapterInactive;
import org.omg.PortableServer.POAPackage.ServantNotActive;
import org.omg.PortableServer.POAPackage.WrongPolicy;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by constie on 07.01.2018.
 */

final class AdjustableSemaphore {

    /**
     * semaphore starts at 0 capacity; must be set by setMaxPermits before use
     */
    private final ResizeableSemaphore semaphore = new ResizeableSemaphore();

    /**
     * how many permits are allowed as governed by this semaphore.
     * Access must be synchronized on this object.
     */
    private int maxPermits = 0;

    /**
     * New instances should be configured with setMaxPermits().
     */
    public AdjustableSemaphore() {
        // no op
    }

    /*
     * Must be synchronized because the underlying int is not thread safe
     */
    /**
     * Set the max number of permits. Must be greater than zero.
     *
     * Note that if there are more than the new max number of permits currently
     * outstanding, any currently blocking threads or any new threads that start
     * to block after the call will wait until enough permits have been released to
     * have the number of outstanding permits fall below the new maximum. In
     * other words, it does what you probably think it should.
     *
     * @param newMax
     */
    synchronized void setMaxPermits(int newMax) {
        if (newMax < 1) {
            throw new IllegalArgumentException("Semaphore size must be at least 1,"
                    + " was " + newMax);
        }

        int delta = newMax - this.maxPermits;

        if (delta == 0) {
            return;
        } else if (delta > 0) {
            // new max is higher, so release that many permits
            this.semaphore.release(delta);
        } else {
            delta *= -1;
            // delta < 0.
            // reducePermits needs a positive #, though.
            this.semaphore.reducePermits(delta);
        }

        this.maxPermits = newMax;
    }

    /**
     * Release a permit back to the semaphore. Make sure not to double-release.
     *
     */
    void release() {
        this.semaphore.release();
    }

    /**
     * Get a permit, blocking if necessary.
     *
     * @throws InterruptedException
     *             if interrupted while waiting for a permit
     */
    void acquire() throws InterruptedException {
        this.semaphore.acquire();
    }

    /**
     * A trivial subclass of <code>Semaphore</code> that exposes the reducePermits
     * call to the parent class. Doug Lea says it's ok...
     * http://osdir.com/ml/java.jsr.166-concurrency/2003-10/msg00042.html
     */
    private static final class ResizeableSemaphore extends Semaphore {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        /**
         * Create a new semaphore with 0 permits.
         */
        ResizeableSemaphore() {
            super(0);
        }

        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }

    public int availablePermits(){
        return this.semaphore.availablePermits();
    }
}


public class Start {
    public static void main(String args[]) throws CannotProceed, InvalidName, NotFound, org.omg.CORBA.ORBPackage.InvalidName, ServantNotActive, WrongPolicy, AdapterInactive {

        ORB orb = ORB.init(args, null );
        POA rootpoa = (POA)orb.resolve_initial_references( "RootPOA" );
        rootpoa.the_POAManager().activate();

        Services services = new Services();
        Server server = new Server(args, services);
        org.omg.CORBA.Object ref = rootpoa.servant_to_reference(server);

        org.omg.CORBA.Object namingContextObj = orb.resolve_initial_references("NameService");
        NamingContext nCont = NamingContextHelper.narrow( namingContextObj );
        NameComponent[] path = {
                new NameComponent( "SERVER", "Object" )
        };
        nCont.rebind( path, ref );
        orb.run();
    }
}

class Client implements Runnable{
    private String[] args;
    private String url;
    private int taskId;

    public Client(String[] args, String url, int taskId){
        this.args = args;
        this.url = url;
        this.taskId = taskId;
    }
    public void run() {
        ORB orb = ORB.init(args, null);
        org.omg.CORBA.Object namingContextObj = null;
        try {
            namingContextObj = orb.resolve_initial_references("NameService");
        } catch (org.omg.CORBA.ORBPackage.InvalidName invalidName) {
            invalidName.printStackTrace();
        }
        NamingContext namingContext = NamingContextHelper.narrow(namingContextObj);

        //unique name
        NameComponent[] path = {
                new NameComponent(url, "Object")
        };
        org.omg.CORBA.Object envObj = null;
        try {
            envObj = namingContext.resolve(path);
        } catch (NotFound notFound) {
            notFound.printStackTrace();
        } catch (CannotProceed cannotProceed) {
            cannotProceed.printStackTrace();
        } catch (InvalidName invalidName) {
            invalidName.printStackTrace();
        }
        // zostaje utworzony obiekt interfejsu zdefiniowanego w idl
        TaskInterface task = TaskInterfaceHelper.narrow(envObj);
        task.start(taskId);
    }
}

class Services {
    private int taskIter = 0;
    private int userIter = 0;

    private ConcurrentSkipListSet completedTasks = new ConcurrentSkipListSet();
    public synchronized int getTaskIter(){
        return taskIter++;
    }
    public synchronized int getUserIter(){
        return userIter++;
    }

    public synchronized void addCompletedTask(int taskId){
        completedTasks.add(taskId);
    }
    public synchronized boolean ifTaskCompleted(int taskId){
        if(completedTasks.contains(taskId)){
            return true;
        } else {
            return false;
        }
    }
}

class TasksService {
    private String[] urls;
    private int tasks;
    private int parallelTasks;
    private int owner;
    private int unprocessedTasks;
    private int taskGroups;
    private String[] args;
    private AdjustableSemaphore semaphore;
    private Services services;
    private int localTaskIter = 0;
    private int status = 0;
    private ConcurrentSkipListSet<Integer> taskIds = new ConcurrentSkipListSet<>();
    private ConcurrentHashMap<Integer, Integer> taskStatus = new ConcurrentHashMap<>();
    public TasksService(Services services, String[] args, String[] urls, int tasks, int parallelTasks, int owner, AdjustableSemaphore semaphore){
        this.urls = urls;
        this.tasks = tasks;
        this.parallelTasks = parallelTasks;
        this.owner = owner;
        this.unprocessedTasks = tasks;
        this.taskGroups = tasks/parallelTasks;
        this.semaphore = semaphore;
        this.args = args;
        this.services = services;

//        for(int i=0; i<tasks; i++){
//            taskStatus.put(i, 0);
//        }
    }
    public synchronized int localTaskIterFactory(){
        return localTaskIter++;
    }
    public int getUserid(){
        return owner;
    }
    public boolean ifContainsTask(int taskId){
        return taskIds.contains(taskId);
    }
    public void tryProcess() throws InterruptedException {
        //if previous called done ->
        //if semafor.availablePermits() >= parallelTasks
        //semaphore.acquire
        if(semaphore.availablePermits() >= parallelTasks && (tasks-unprocessedTasks)%parallelTasks == 0 && unprocessedTasks != 0 && status == 0){

            status = 1;
            for(int i=0; i<parallelTasks; i++) {
                semaphore.acquire();
                int iter = services.getTaskIter();
                taskIds.add(iter);
                int locIter = localTaskIterFactory();
                Thread th = new Thread(new Client(args, urls[locIter], iter));
                th.start();
            }
        }
    }
    public void cancel(){
        unprocessedTasks = 0;
    }
    public boolean tryRemove(int taskId){
        unprocessedTasks--;
        if((tasks - unprocessedTasks)%parallelTasks == 0){
            status = 0;
        }
//        taskStatus.put(taskId, 1);
        if(unprocessedTasks == 0){
            return true;
        } else {
//            tryProcess();
            return false;
        }
    }

}

class Server extends ServerInterfacePOA {
    private String[] args;
    private Services services;

    private ConcurrentLinkedQueue<TasksService> tasksList = new ConcurrentLinkedQueue<>();
    public AdjustableSemaphore semaphore = new AdjustableSemaphore();
//    private ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
//    private BlockingQueue<Task> processList = new LinkedBlockingQueue<>();

    Server(String[] args, Services services){
        this.args = args;
        this.services = services;
    }

    @Override
    public void setResources(int cores) {
        semaphore.setMaxPermits(cores);
    }

    @Override
    public void submit(String[] urls, int tasks, int parallelTasks, IntHolder userID) {
        int temp = services.getUserIter();
//        Integer temp = ff.getAndIncrement();
        userID.value = temp;
        tasksList.add(new TasksService(services, args, urls, tasks, parallelTasks, temp, semaphore));

        try {
            analyze();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void done(int taskID) {
        //get info that task completed
        //semaphore.release
        semaphore.release();
//        services.addCompletedTask(taskID);
        for(TasksService task : tasksList){
            if(task.ifContainsTask(taskID)) {
                if (task.tryRemove(taskID)) {
                    task = null;
                    break;
                } else {
                    try {
                        task.tryProcess();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
        }
        for(TasksService task : tasksList){
            try {
                task.tryProcess();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel(int userID) {
        //receive cancel instruction
        for(TasksService task : tasksList){
            if(task.getUserid() == userID){
                task.cancel();
                break;
            }
        }
    }

    public void analyze() throws InterruptedException {
//        synchronized (tasksList) {
            for (TasksService task : tasksList) {
                task.tryProcess();
            }
//        }
    }
}

