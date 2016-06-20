/*************************************************************************
  > File Name: threadPoll.cpp
  > Author: tiankonguse(skyyuan)
  > Mail: i@tiankonguse.com 
  > Created Time: 2016年06月 5日 19:31:25
***********************************************************************/

#include<iostream>
#include<cstdio>
#include<cstring>
#include<cstdlib>
#include<string>
#include<queue>
#include<map>
#include<cmath>
#include<stack>
#include<algorithm>
#include<functional>
#include<stdarg.h>
using namespace std;
#ifdef __int64
typedef __int64 LL;
#else
typedef long long LL;
#endif

extern "C" void * _run_thread ( void * arg ){ 
    if (arg != NULL){
        ((TThread*) arg)->run();
    }
    return NULL;
}

class TCondition;

class TMutex{
    friend class TCondition;
    protected:
    // the mutex itself and the mutex attribute
    pthread_mutex_t _mutex;
    pthread_mutexattr_t _mutex_attr;
public:
    TMutex (){ 
        pthread_mutexattr_init( & _mutex_attr );
        pthread_mutex_init( & _mutex, & _mutex_attr );
    }
    ~TMutex (){ 
        pthread_mutex_destroy( & _mutex );
        pthread_mutexattr_destroy( & _mutex_attr );
    }
    // lock and unlock mutex (return 0 on success)
    int lock () { 
        return pthread_mutex_lock( & _mutex ); 
    }
    int unlock () { 
        return pthread_mutex_unlock( & _mutex ); 
    }
    // try a lock (return 0 if lock was successful)
    int trylock () { 
        return pthread_mutex_trylock( & _mutex ); 
    }
};


class TCondition{
protected:
    // the Pthread condition variable
    pthread_cond_t _cond;
public:
    // constructor and destructor
    TCondition () { 
        pthread_cond_init( & _cond, NULL ); 
    }
    ~TCondition () { 
        pthread_cond_destroy( & _cond ); 
    }
    // condition variable related methods
    void wait ( TMutex & m ){ 
        pthread_cond_wait( & _cond, & m._mutex ); 
    }
    void signal (){ 
        pthread_cond_signal( & _cond ); 
    }
    void broadcast (){ 
        pthread_cond_broadcast( & _cond ); 
    }
};

class TThread {
    
protected:
    pthread_t _thread_id; // ID of the thread
    pthread_attr_t _thread_attr; // thread attributes
    bool _running; // true if thread is running
    int _thread_no; // opt. number of thread
    
public:
    // constructor and destructor
    TThread ( int p = -1 ): _running(false), _thread_no(p){
        
    }
    virtual ~TThread (){
        if ( _running ){
            cancel();
        }
    }
    
    // access local data
    int thread_no () const { 
        return _thread_no; 
    }
    void set_thread_no ( int p ) { 
        _thread_no = p; 
    }
    bool on_proc ( int p ) const{
        if ((p == -1) || (_thread_no == -1) || (p == _thread_no)){
            return true;        
        }else{
            return false
        }
    }
    
    // user interface
    virtual void run () = 0;
    virtual void start ( bool d = false,bool s = false ) { 
        create( d, s ); 
    }
    virtual void stop () { 
        cancel(); 
    }
    
    // thread management
    /*
     * d: joinable or detached
     * s: contention scope
     *    - true: process contention scope
     *    - false: system contention scope
     */
    void create ( bool detached = false, bool sscope = false){
        if ( ! _running ){
            int status;
            pthread_attr_init( & _thread_attr );
            if ( detached ){
                pthread_attr_setdetachstate( & _thread_attr, PTHREAD_CREATE_DETACHED );
            }
                
            if ( sscope ){
                pthread_attr_setscope( & _thread_attr, PTHREAD_SCOPE_SYSTEM );
            }
                
            pthread_create( & _thread_id, & _thread_attr, _run_thread, this);
            _running = true;
        }else{
            printf( "ERROR : thread is already running\n" );
        }
        
    }
    void join ();
    void cancel ();
};


class TJob{
protected:
    // number of processor this job was assigned to
    int _job_no;
    // associated thread in thread pool
    TPoolThr * _pool_thr;
public:
    // constructor
    TJob ( int p ) : _job_no(p), _pool_thr(NULL) {
        
    }
    // running method
    virtual void run ( void * ptr ) = 0;
    // access local data
    int job_no () const { 
        return _job_no; 
    }
    TPoolThr * pool_thr () { 
        return _pool_thr; 
    }
    void set_pool_thr ( TPoolThr * t ) { 
        _pool_thr = t; 
    }
    // compare if given processor is local 
    bool on_proc ( int p ) const{
        if ((p == -1) || (_job_no == -1) || (p == _job_no)){
            return true;        
        }else{
            return false
        }
    }
};


class TPoolThr : public TThread{
protected:
    // thread pool we belong to
    TThreadPool * _pool;
    
    // job to run and data for the job
    TJob * _job;
    void * _data_ptr;
    
    // condition and mutex for waiting for job
    // and pool synchronisation
    TCondition _work_cond, _sync_cond;
    TMutex _work_mutex, _sync_mutex;
    
    // indicates work-in-progress, end-of-thread
    // and a mutex for the local variables
    bool _work, _end;
    TMutex _var_mutex;
    
    // mutex for synchronisation with destructor
    TMutex _del_mutex;
    
    // should the job be deleted upon completion
    bool _del_job;
public:
    // constructor and destructor
    TPoolThr ( int n, TThreadPool * p );
    
    // running method
    void run (){
        _del_mutex.lock();
        while ( ! _end ){
            // wait for work
            _work_mutex.lock();
            while ((_job == NULL) && ! _end ){
                _work_cond.wait( _work_mutex );
            }
            
            _work_mutex.unlock();
            // check again if job is set and execute it
            if ( _job != NULL ){
                _job->run( _data_ptr );
                // detach thread from job
                _job->set_pool_thr( NULL );
                if ( _del_job ){
                    delete _job;
                }
                
                set_job( NULL, NULL );
                _sync_mutex.unlock();
            }
            // append thread to idle list
            _pool->append_idle( this );
        }
        _del_mutex.unlock();
    }
    
    // access local variables
    void set_end ( bool f );
    void set_work ( bool f );
    void set_job ( TJob * j, void * p, bool del );
    
    bool is_working () const { 
        return _job != NULL; 
    }
    TJob * job () { 
        return _job; 
    }
    TCondition & work_cond () { 
        return _work_cond; 
    }
    TMutex & work_mutex () { 
        return _work_mutex; 
    }
    TCondition & sync_cond () { 
        return _sync_cond; 
    }
    TMutex & sync_mutex () { 
        return _sync_mutex; 
    }
    TMutex & del_mutex () { 
        return _del_mutex; 
    }
};


class TThreadPool{
public:
    protected:
    // maximum degree of parallelism
    uint _max_parallel;
    // array of threads, handled by pool
    TArray< TPoolThr * > _threads;
    
    // list of idle threads, mutices and condition for it
    TSLL< TPoolThr * > _idle_threads;
    TMutex _idle_mutex, _list_mutex;
    TCondition _idle_cond;
public:
    // constructor and destructor
    TThreadPool ( uint max_p ){
        _max_parallel = max_p;
        _threads.set_size( max_p );
        for ( uint i = 0; i < max_p; i++ ){
            _threads[i] = new TPoolThr( i, this );
            _idle_threads.append( _threads[i] );
            _threads[i]->start( true, true );
        }
        // pthread_setconcurrency( max_p + pthread_getconcurrency() );
    }

    ~TThreadPool () {
        sync_all();
        for ( uint i = 0; i < _max_parallel; i++ ){
            _threads[i]->sync_mutex().lock();
            _threads[i]->set_end( true );
            _threads[i]->set_job( NULL, NULL );
            _threads[i]->work_mutex().lock();
            _threads[i]->work_cond().signal();
            _threads[i]->work_mutex().unlock();
            _threads[i]->sync_mutex().unlock();
        }
        // cancel still pending threads and delete them all
        for ( uint i = 0; i < _max_parallel; i++ ){
            _threads[i]->del_mutex().lock();
            delete _threads[i];
        }
    }
    
    
    // access local variables
    uint max_parallel () const { 
        return _max_parallel; 
    }
    
    // run, stop and synchronise with job
    void run ( TJob * job, void * ptr = NULL, bool del = false ){
        TPoolThr * t = get_idle();
        // and start the job
        t->sync_mutex().lock();
        t->set_job( job, ptr, del );
        // attach thread to job
        job->set_pool_thr( t );
        t->work_mutex().lock();
        t->work_cond().signal();
        t->work_mutex().unlock();
    }
    void sync ( TJob * job ){
        if ( job == NULL ){
            return;
        }
        
        TPoolThr * t = job->pool_thr();
        // check if job is already released
        if ( t == NULL ){
            return;
        }
        
        // look if thread is working and wait for signal
        t->sync_mutex().lock();
        t->set_job( NULL, NULL );
        t->sync_mutex().unlock();
        // detach job and thread
        job->set_pool_thr( NULL );
    }
    void sync_all (){
        for ( uint i = 0; i < _max_parallel; i++ ){
            if ( _threads[i]->sync_mutex().trylock() ){
                _threads[i]->sync_mutex().lock();
            }
            _threads[i]->sync_mutex().unlock();
        }
    }
    // return idle thread form pool
    TPoolThr * get_idle (){
        while ( true ){
            // wait for an idle thread
            _idle_mutex.lock();
            while ( _idle_threads.size() == 0 ){
                _idle_cond.wait( _idle_mutex );
            }
            _idle_mutex.unlock();
            
            // get first idle thread
            _list_mutex.lock();
            if ( _idle_threads.size() > 0 ){
                TPoolThr * t = _idle_threads.behead();
                _list_mutex.unlock();
                return t;
            }
            _list_mutex.unlock();
        }
    }
    // insert idle thread into pool
    void append_idle ( TPoolThr * t );
};
