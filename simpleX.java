package wc;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.LongValue;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class simpleX {
    //c：优化目标
    public static Double[] c = {0.0, 1.0, 1.0, 0.0, 0.0};
    public static Double[] result;

    public static void main(String[] argv) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        List<Double[]> _pivotRow = new ArrayList<>();
        _pivotRow.add(new Double[] {12.0, 2.0, 1.0, 1.0, 0.0});

        List<Double[]> _A = new ArrayList<>();
        //A：约束条件
        _A.add(new Double[] {12.0, 2.0, 1.0, 1.0, 0.0});
        _A.add(new Double[] {9.0, 1.0, 2.0, 0.0, 1.0});

        DataSet<Double[]> A = env.fromCollection(_A);
        DataSet<Double[]> pivotRow = env.fromCollection(_pivotRow);

        IterativeDataSet<Double[]> loop = pivotRow.iterate(20)
                .registerAggregationConvergenceCriterion("negtiveCNum", new LongSumAggregator(),new UpdatedElementsConvergenceCriterion());

        DataSet<Double[]> newRow = A
                .map(new applyPivot())
                .withBroadcastSet(loop,"pivotRow")
                .reduce(new choosePivot());

        DataSet<Double[]> finalA = loop.closeWith(newRow);

        finalA.print();
        System.out.print("Maximum: ");
        System.out.println(0-result[0]);
    }

    public static class applyPivot extends RichMapFunction<Double[], Double[]> {
        private Double[] tmpC = c.clone();
        private Double[] tmpPivot;
        private Integer cid;
        private Integer nextCid;
        private LongSumAggregator cPosition;

        @Override
        public void open(Configuration parameters) throws Exception {
            cPosition = getIterationRuntimeContext().getIterationAggregator("negtiveCNum");
            tmpPivot= (Double[])getRuntimeContext().getBroadcastVariable("pivotRow").iterator().next();
            //cid is oldCid 本轮计算的入基
            //应该等于nextCid
            for (int i = 1 ; i < tmpC.length ; i++){
                if (tmpC[i] > 0) {
                    cid = i;
                    break;
                }
            }
            //更新优化目标c
            Double scale = (Double)(tmpC[cid]/tmpPivot[cid]);
            for (int i = 0 ; i < tmpC.length ; i++){
                tmpC[i] = tmpC[i] - tmpPivot[i] * scale;
            }
            //更新nextCid
            nextCid = 0;
            for (int i = 0 ; i < tmpC.length ; i++){
                if (tmpC[i] > 0) {
                    nextCid = i;
                    cPosition.aggregate(nextCid);
                }
            }
            result = tmpC;
            //优化目标和新基在每个subtask计算结果相同

            super.open(parameters);
        }

        @Override
        public Double[] map(Double[] doubles) throws Exception {
            if(nextCid==0)
                return  doubles;
            //apply pivot
            if(Arrays.equals(doubles,tmpPivot)){
                Double scale = 1.0/doubles[cid];
                for(int i = 0 ; i < doubles.length ; i++)
                    doubles[i] = doubles[i] * scale;
            }
            else if(doubles[cid] != 0) {
                Double scale = doubles[cid] / tmpPivot[cid];
                for(int i = 0 ; i < doubles.length ; i++)
                    doubles[i] -= tmpPivot[i] * scale;
            }
            //计算下一轮的新基的每行约束
            if(doubles[nextCid]>0) {
                Double scale = doubles[nextCid];
                for (int i = 0; i < doubles.length; i++)
                    doubles[i] /= scale;
            }
            return doubles;
        }
    }

    public static class choosePivot implements ReduceFunction<Double[]>{
        @Override
        public Double[] reduce(Double[] doubles, Double[] t1) throws Exception {
            //选择最强的约束
            if(doubles[0]<t1[0])
                return doubles;
            return t1;
        }
    }

    private static class UpdatedElementsConvergenceCriterion implements ConvergenceCriterion<LongValue> {
        @Override
        public boolean isConverged(int iteration, LongValue value) {
            return value.getValue() < 1;
        }
    }

}
