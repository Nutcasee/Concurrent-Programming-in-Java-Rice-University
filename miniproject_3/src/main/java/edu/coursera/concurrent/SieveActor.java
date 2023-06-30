package edu.coursera.concurrent;

import java.util.ArrayList;
import java.util.List;

import edu.rice.pcdp.Actor;
import edu.rice.pcdp.PCDP;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     *
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
//        throw new UnsupportedOperationException();
    	final SieveActorActor actor = new SieveActorActor(2);

        PCDP.finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                actor.send(i);
            }
            actor.send(0);
        });
        
     // Sum up the number of local primes from each actor in the chain.
        int totalPrimes = 0;
        SieveActorActor currentActor = actor;
        while (currentActor != null) {
            totalPrimes += currentActor.numLocalPrimes();
            currentActor = currentActor.nextActor;
        }

        return totalPrimes;
        
//    	final List<Integer> localPrimes = new ArrayList<Integer>();
//        localPrimes.add(2);
//        for (int i = 3; i <= limit; i += 2) {
//            checkPrime(i, localPrimes);
//        }
//
//        return localPrimes.size();
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
    	private static final int MAX_LOCAL_PRIMES = 1000;
    	private final int localPrimes[];
    	private int numLocalPrimes;
    	private SieveActorActor nextActor;
    	
    	SieveActorActor(final int localPrime){    	    
    	    this.localPrimes = new int[MAX_LOCAL_PRIMES];
    	    this.localPrimes[0] = localPrime;
    	    this.numLocalPrimes = 1;
    	    this.nextActor = null;    	    
    	}
    	
    	public SieveActorActor nextActor() {
    	    return nextActor;
    	}
    	
    	public int numLocalPrimes() {
    	    return numLocalPrimes;
    	}
    	
    	/**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
	    @Override
        public void process(final Object msg) {            
            final int candidate = (Integer) msg;            
            if(candidate <=0) {        	
	        	if(nextActor != null) {        	    
	        	    nextActor.send(msg);        	    
	        	}
	        	return;
	        	//exit();        	
            }else {        	
	        	final boolean locallyPrime = isLocallyPrime(candidate);
	        	if(locallyPrime) {        	    
	        	    if(numLocalPrimes < MAX_LOCAL_PRIMES) {
		          		localPrimes[numLocalPrimes] = candidate;
		        		numLocalPrimes +=1;        		
	        	    }else if(nextActor == null){        		
		        		nextActor = new SieveActorActor(candidate);
		        		//nextActor.start();
	        	    }else {        		
	        	    	nextActor.send(msg);
	        	    }        	    
	        	}
            }
        }
        
        private boolean isLocallyPrime(final int candidate) {                
            final boolean[] isPrime = {true};
            checkPrimeKernel(candidate,isPrime,0,numLocalPrimes);
            return isPrime[0];
        }
        
        private void checkPrimeKernel(final int candidate, final boolean[] isPrime, 
        		final int startIndex,final int endIndex ) {
            for(int i = startIndex; i < endIndex; i++) {
	        	if(candidate % localPrimes[i] == 0) {            	    
	        	    isPrime[0] = false;
	        	    //break;            	    
	        	}            	
            }              
        }           
    }
}
