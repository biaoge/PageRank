
public class Driver {

    //同来调动两个MapReduce的main函数
    public static void main(String[] args) throws Exception {

        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        //注意都是dir，而不是直接的文件
        //args0: dir of transition.txt
        //args1: dir of PageRank.txt

        //第一个MR的output所存储的文件夹
        //args2: dir of unitMultiplication result


        //args3: times of convergence


        String transitionMatrix = args[0];
        String prMatrix = args[1];  // 传入dir，不用带数字后缀。第一次是/input/pr0，第二次就是/input/pr1...
        String unitState = args[2]; // /ouput/subPR
        //count表示迭代的次数
        int count = Integer.parseInt(args[3]);


        //循环实现动态迭代
        for(int i=0;  i<count;  i++) {
            //指定MapReduce1输入
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};
            //MapReduce1
            multiplication.main(args1);

            //指定MapReduce2输入
            String[] args2 = {unitState + i, prMatrix+(i+1)};
            //MapReduce2
            sum.main(args2);
        }
    }
}
