for i in `seq 1 9`;
do
    ssh -tt teng9@fa16-cs425-g06-0${i}.cs.illinois.edu "killall node" &
done

wait

ssh -tt teng9@fa16-cs425-g06-10.cs.illinois.edu "killall node" &

