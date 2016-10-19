for i in `seq 1 9`;
do
    scp install_node.sh teng9@fa16-cs425-g06-0${i}.cs.illinois.edu:~/install_node.sh &
done

wait

for i in `seq 1 9`;
do
    ssh -tt teng9@fa16-cs425-g06-0${i}.cs.illinois.edu "stty raw -echo; echo ${PW} | sudo -S ./install_node.sh" &
done

wait

scp install_node.sh teng9@fa16-cs425-g06-10.cs.illinois.edu:~/install_node.sh
ssh -tt teng9@fa16-cs425-g06-10.cs.illinois.edu "stty raw -echo; echo ${PW} | sudo -S ./install_node.sh"

