---------------------- MODULE KafkaQueue -----------------------------
EXTENDS Naturals

CONSTANT topic
CONSTANT alphabet
VARIABLES enqueued, dequeued, messages

Init ==
    /\ topic' = topic
    /\ enqueued = 0
    /\ dequeued = 0
    /\ messages = {}

Enqueue(message) ==
    /\ enqueued' = enqueued + 1
    /\ messages' = messages \cup { message }
    /\ UNCHANGED <<topic, dequeued>>

Dequeue ==
    /\ dequeued' = dequeued + 1
    /\ IF messages /= {} THEN
          /\ LET chosen_message == CHOOSE m \in messages : TRUE
             IN
                /\ messages' = messages \ { chosen_message }
       ELSE
          /\ messages' = messages
    /\ UNCHANGED <<topic, enqueued>>


Producer ==
    /\ \E message \in alphabet :
            Enqueue(message)

Consumer ==
    /\ Dequeue

Next ==
    \/ Producer
    \/ Consumer

QueueIntegrity ==
    /\ enqueued \geq dequeued

MessageConsistency ==
    /\ messages' \subseteq messages

THEOREM QueueIntegrity
THEOREM MessageConsistency

=======================================================================
