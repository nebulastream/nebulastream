#var thread
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 1 --numCampaings 100 > thread.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 2 --numCampaings 100 >> thread.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 4 --numCampaings 100 >> thread.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 8 --numCampaings 100 >> thread.out 

exit
#var campaing
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 1 --numCampaings 1 > campaing.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 1 --numCampaings 10 >> campaing.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 1 --numCampaings 100 >> campaing.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 1 --numCampaings 1000 >> campaing.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 1 --numCampaings 10000 >> campaing.out 

#var intups
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 100 --numThreads 1 --numCampaings 100 > intups.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 1000 --numThreads 1 --numCampaings 100 >> intups.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 10000 --numThreads 2 --numCampaings 100 >> intups.out 
/home/zeuchste/git/IoTDB/iotdb/build/tests/ysb_performance_test --numBuffers 100000 --numThreads 1 --numCampaings 100 >> intups.out 
