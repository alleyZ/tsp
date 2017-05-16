package com.alleyz.nlpir;

import com.alleyz.tsp.nplir.NLPIRUtil;

/**
 * Created by zhaihw on 2017/5/4.
 *
 */
public class TestNlpir {
    public static void main(String[] args) {

        String line = "我们知道，使用JNI调用.dll/.so共享类库是非常非常麻烦和痛苦的。" +
                "如果有一个现有的.dll/.so文件，如果使用JNI技术调用，我们首先需要另外使用C语言写一个.dll/.so共享库，使用SUN规定的数据结构替代C语言的数据结构，调用已有的  dll/so中公布的函数。" +
                "然后再在Java中载入这个适配器dll/so，再编写Java   native函数作为dll中函数的代理。" +
                "经过2个繁琐的步骤才能在Java中调用本地代码。" +
                "因此，很少有Java程序员愿意编写调用dll/.so库中的原生函数的java程序。这也使Java语言在客户端上乏善可陈。可以说JNI是Java的一大弱点";

        System.out.println(NLPIRUtil.getWordCount(line));
        System.out.println(NLPIRUtil.segment(line, false));
        System.out.println(NLPIRUtil.segment(line, true));
        System.out.println(NLPIRUtil.segmentMin(line));
        System.out.println(NLPIRUtil.segment(getPath("/origin.file"), "d:\\seg.file", true));

        System.out.println(NLPIRUtil.importUserDic(getPath("/user.dic"), false));
        System.out.println(NLPIRUtil.segment(line, false));
        System.out.println(NLPIRUtil.importUserDic(getPath("/user2.dic"), true));
        System.out.println(NLPIRUtil.segment(line, false));

        System.out.println(NLPIRUtil.importKeyBlackList(getPath("/black.dic")));
        System.out.println(NLPIRUtil.segment(line, false));

        System.out.println(NLPIRUtil.addUserWord("共享类库  kkkl"));
        System.out.println(NLPIRUtil.segment(line, true));

        System.out.println(NLPIRUtil.saveUserDic());

        System.out.println(NLPIRUtil.delUserWord("我们知道"));
        System.out.println(NLPIRUtil.delUserWord("共享类库"));
        System.out.println(NLPIRUtil.segment(line, true));

        System.out.println(NLPIRUtil.getWordProb("共享") + "");

        System.out.println(NLPIRUtil.coreDicHasTheWord("调用"));
        System.out.println(NLPIRUtil.coreDicHasTheWord("我们知道"));
        System.out.println(NLPIRUtil.addUserWord("共享库  kkkl"));
        System.out.println(NLPIRUtil.coreDicHasTheWord("共享库"));

        System.out.println(NLPIRUtil.getWordNature("经济"));

        System.out.println(NLPIRUtil.getKeyWords(line, 5, true));

        System.out.println(NLPIRUtil.getNewWords(line, 50, true));

        System.out.println(NLPIRUtil.fingerprint(line));
        System.out.println(NLPIRUtil.fingerprint(line+"1"));
        System.out.println(NLPIRUtil.fingerprint("我们是众人"));
        System.out.println(NLPIRUtil.segment(line, true));
        System.out.println(NLPIRUtil.setPOSTag(NLPIRUtil.PosMap.PKU_POS_MAP_FIRST));
        System.out.println(NLPIRUtil.segment(line, true));
        System.out.println(NLPIRUtil.setPOSTag(NLPIRUtil.PosMap.ICT_POS_MAP_FIRST));

        System.out.println(NLPIRUtil.getEngWordOrigin("made"));

        System.out.println(NLPIRUtil.getWordFreqCount(line));
        System.out.println(NLPIRUtil.isInit());

    }
    private static String getPath(String file) {
        return TestNlpir.class.getResource(file).getPath().substring(1);
    }
}
