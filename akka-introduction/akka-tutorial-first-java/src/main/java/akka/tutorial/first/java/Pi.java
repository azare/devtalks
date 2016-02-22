package akka.tutorial.first.java;

import akka.actor.*;
import akka.routing.RoundRobinRouter;
import akka.util.Duration;

import java.util.concurrent.TimeUnit;

public class Pi
{
    public static void main( String[] args )
    {
        Pi pi = new Pi();
        pi.calculate(4, 10000, 10000);
    }

    public static void calculate(final int numOfWorkers, final int numOfElements, final int numOfMessages) {

        ActorSystem system = ActorSystem.create("PiSystem");

        final ActorRef listener = system.actorOf(new Props(Listener.class), "listener");

        ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new Master(numOfWorkers, numOfMessages, numOfElements, listener);
            }
        }), "master");

        master.tell(new Calculate());

    }

    static class Calculate {}

    static class Work {
        private final int start;
        private final int numOfElements;

        public Work(int start, int numOfElements) {
            this.start = start;
            this.numOfElements = numOfElements;
        }

        public int getStart() {
            return start;
        }

        public int getNumOfElements() {
            return numOfElements;
        }
    }

    static class Result {
        private final double value;

        public Result(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }
    }

    static class PiApproximation {
        private final double pi;
        private final Duration duration;

        public PiApproximation(double pi, Duration duration) {
            this.pi = pi;
            this.duration = duration;
        }

        public double getPi() {
            return pi;
        }

        public Duration getDuration() {
            return duration;
        }
    }

    public static class Worker extends UntypedActor {

        public void onReceive(Object message) {
            if (message instanceof Work) {
                Work work = (Work) message;
                double result = calculatePiFor(work.getStart(), work.getNumOfElements());
                getSender().tell(new Result(result), getSelf());
            }
            else {
                unhandled(message);
            }
        }

        private double calculatePiFor(int start, int numOfElements) {
            double acc = 0.0;
            for (int i = start * numOfElements; i<= ((start + 1) * numOfElements -1); i++) {
                acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
            }
            return acc;
        }
    }

    public static class Master extends UntypedActor {

        private final int numOfMessages;
        private final int numOfElements;

        private double pi;
        private int numOfResults;
        private final long start = System.currentTimeMillis();

        private final ActorRef listener;
        private final ActorRef workerRouter;

        public Master(
                final int numOfWorkers,
                int numOfMessages,
                int numOfElements,
                ActorRef listener) {

            this.numOfMessages = numOfMessages;
            this.numOfElements = numOfElements;
            this.listener = listener;

            workerRouter = this.getContext().actorOf(new Props(Worker.class).withRouter(
                    new RoundRobinRouter(numOfWorkers)), "workerRouter");
        }

        public void onReceive(Object message) {
            if (message instanceof Result) {
                Result result = (Result) message;
                pi += result.getValue();
                numOfResults += 1;
                if (numOfResults == numOfMessages) {
                    Duration duration = Duration.create(System.currentTimeMillis() - start, TimeUnit.MICROSECONDS);
                    listener.tell(new PiApproximation(pi, duration), getSelf());
                    getContext().stop(getSelf());
                }
            }
            else if (message instanceof Calculate) {
                for (int start = 0; start < numOfMessages; start++) {
                    workerRouter.tell(new Work(start, numOfElements), getSelf());
                }
            }
            else {
                unhandled(message);
            }
        }
    }
    public static class Listener extends UntypedActor {
        public void onReceive(Object message) {
            if (message instanceof PiApproximation) {
                PiApproximation approximation = (PiApproximation) message;
                System.out.println(String.format("\n\tPi approximation: " +
                                "\t\t%s\n\tCalculation time: \t%s",
                        approximation.getPi(), approximation.getDuration()));
                getContext().system().shutdown();
            } else {
                unhandled(message);
            }
        }
    }
}
