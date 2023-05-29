
echo -e "Step 1: Build your code using mvn test, thus running your Junit tests" >& >(tee output.log)

mkdir logs
rm logs/*

mvn clean test &> >(tee -a output.log)
# mvn clean test &> >(tee -a output.log)

rm logs/*


echo -e "Step 2: Create a cluster of 7 nodes and one gateway, starting each in their own JVM" &> >(tee -a output.log)
cd target/classes


java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 0 8000 7 1 7 1 localhost 8010 2 localhost 8020 3 localhost 8030 4 localhost 8040 5 localhost 8050 6 localhost 8060 7 localhost 8072  &> >(tee -a ../../output.log) &
server0=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 1 8010 7 1 7 0 localhost 8000 2 localhost 8020 3 localhost 8030 4 localhost 8040 5 localhost 8050 6 localhost 8060 7 localhost 8072  &> >(tee -a ../../output.log) &
server1=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 2 8020 7 1 7 1 localhost 8010 0 localhost 8000 3 localhost 8030 4 localhost 8040 5 localhost 8050 6 localhost 8060 7 localhost 8072  &> >(tee -a ../../output.log) &
server2=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 3 8030 7 1 7 1 localhost 8010 2 localhost 8020 0 localhost 8000 4 localhost 8040 5 localhost 8050 6 localhost 8060 7 localhost 8072  &> >(tee -a ../../output.log) &
server3=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 4 8040 7 1 7 1 localhost 8010 2 localhost 8020 3 localhost 8030 0 localhost 8000 5 localhost 8050 6 localhost 8060 7 localhost 8072  &> >(tee -a ../../output.log) &
server4=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 5 8050 7 1 7 1 localhost 8010 2 localhost 8020 3 localhost 8030 4 localhost 8040 0 localhost 8000 6 localhost 8060 7 localhost 8072  &> >(tee -a ../../output.log) &
server5=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 6 8060 7 1 7 1 localhost 8010 2 localhost 8020 3 localhost 8030 4 localhost 8040 5 localhost 8050 0 localhost 8000 7 localhost 8072  &> >(tee -a ../../output.log) &
server6=$!

java -Dlogdir="../../logs/" edu.yu.cs.com3800.stage5.GatewayServer 8070 8072 7 7 0 localhost 8000 1 localhost 8010 2 localhost 8020 3 localhost 8030 4 localhost 8040 5 localhost 8050 6 localhost 8060  &> >(tee -a ../../output.log) &
gateway=$!

# echo $server0 
# echo $server1
# echo $server2
# echo $server3
# echo $server4
# echo $server5
# echo $server6
# echo $gateway


sleep 10

# Send the request and store the response in a variable
# until the response is not "Gateway does not have a leader yet"
echo -e "Step 3: print out the list of server IDs and their roles"  | tee -a ../../output.log
response="Gateway does not have a leader yet"
url="http://localhost:8070/getcluster"
while [[ "$response" == "Gateway does not have a leader yet" ]]; do
  response=$(curl -s "$url")
  sleep 5
done

echo "Gateway's cluster report: $response"  | tee -a ../../output.log

echo -e "Step 4: send 9 client requests"  | tee -a ../../output.log
java edu.yu.cs.com3800.stage5.RequestSender 9 localhost 8070 &> >(tee -a ../../output.log)

echo -e "Step 5a: kill -9 a follower JVM, printing out which one you are killing"  | tee -a ../../output.log
echo "Killing a follower with id 0"  | tee -a ../../output.log
kill -9 $server0

sleep 30

echo -e "Step 5b: display the list of nodes from the Gateway. The dead node should not be on the list"  | tee -a ../../output.log
# response=$(curl -s "$url")
# echo "Gateway's cluster report: $response"  | tee -a ../../output.log
# echo "Gateway's cluster report: helloooooo"  | tee -a ../../output.log
response="Gateway does not have a leader yet"
while [ "$response" == "Gateway does not have a leader yet" ] || [ -z "$response" ]; do
  response=$(curl -s -m 1 "$url")
  sleep 5
done
echo "Gateway's cluster report: $response" | tee -a ../../output.log

sleep 30
echo -e "Step 6a: kill -9 the leader JVM"  | tee -a ../../output.log
echo "Killing the leader with id 6"  | tee -a ../../output.log
kill -9 $server6

sleep 1

echo -e "Step 6b: Send/display 9 more client requests to the gateway, in the background"  | tee -a ../../output.log
java edu.yu.cs.com3800.stage5.RequestSender 9 localhost 8070 > requestsRound2.log & 
requestSender=$!

sleep 40

echo -e "Step 7a: print out the node ID of the leader" | tee -a ../../output.log
response="Gateway does not have a leader yet"
while [[ "$response" == "Gateway does not have a leader yet" ]]; do
  response=$(curl -s "$url")
  sleep 5
done
echo "Gateway's cluster report: $response" | tee -a ../../output.log

wait $requestSender
echo -e "Step 7b: Print out the responses the client receives from the Gateway for the 9 requests sent in step 6"  | tee -a ../../output.log
cat requestsRound2.log  | tee -a ../../output.log

echo -e "Step 8: Send/display 1 more client request (in the foreground)"  | tee -a ../../output.log 
java edu.yu.cs.com3800.stage5.RequestSender 1 localhost 8070   &> >(tee -a ../../output.log)

echo -e "Step 9: List the paths to files containing the Gossip messages received by each node"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8000.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8010.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8020.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8030.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8040.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8050.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8060.log"  | tee -a ../../output.log
echo "stage5/logs/edu.yu.cs.com3800.stage5.GossipVerboseLog-UDPPort-8070.log"  | tee -a ../../output.log


# kill $server0
kill $server1
kill $server2
kill $server3
kill $server4
kill $server5
# kill $server6
kill $gateway
