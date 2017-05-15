package com.alleyz.nplir;


import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.alleyz.nplir.NLPIRUtil.EnCode.UTF8_CODE;
import static java.io.File.separator;

/**
 * Created by zhaihw on 2017/5/3.
 *
 */
public class NLPIRUtil {
    private static CLibrary instance;
    private static final String OS_ARCH = "os.arch"; // 操作系统架构
    private static final String OS_NAME = "os.name"; // 操作系统名称
    private static final String OS_WIN_PREFIX = "win"; // windows操作系统前缀
    private static final String OS_LINUX = "linux"; // windows操作系统前缀
    private static final String OS_ARCH_64 = "64"; // 64位架构
    private static final String OS_ARCH_32 = "32"; // 32位架构
    private static final String NLPIR_LIB_WIN = "NLPIR.dll";
    private static final String NLPIR_LIB_LINUX = "libNLPIR.so";
    private static String NLPIR_DATA_PATH;

    private static volatile boolean isInit = false;

    static {
        buildInstance();
        init(UTF8_CODE);
    }

    /**
     * 字符集编码枚举
     */
    public enum EnCode{
        // gnk  utf8  big5
        GBK_CODE(0), UTF8_CODE(1), BIG5_CODE(2),
        // gbk繁体  utf8繁体
        GBK_FT_CODE(3), UTF8_FT_CODE(4);
        EnCode(int val) {
            this.val = val;
        }
        int val;
    }
    public enum PosMap{
        //计算所一级标注集 计算所二级标注集
        ICT_POS_MAP_FIRST(1), ICT_POS_MAP_SECOND(0),
        //北大二级标注集  北大一级标注集
        PKU_POS_MAP_SECOND(2), PKU_POS_MAP_FIRST(3);
        int val;
        PosMap(int val) {
            this.val = val;
        }
    }

    /**
     * 初始化分词器
     * @param enCode 编码
     */
    public synchronized static boolean init(EnCode enCode) {
        if(!isInit()) {
            isInit = true;
            return instance.NLPIR_Init(NLPIR_DATA_PATH, enCode.val, 0) == 1;
        }
        return isInit;
    }
    public static boolean isInit() {
        return isInit && instance.NLPIR_Init();
    }

    /**
     * 分词
     * @param line 原始语句
     * @param isTag 是否标注
     * @return 空格隔开， 如果有标注则在词之后加词性
     */
    public static String segment(String line, boolean isTag) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_ParagraphProcess(line, isTag ? 1 : 0);
    }

    /**
     * 最小粒度分词
     * @param line 原始语句
     * @return 空格隔开的词语
     */
    public static String segmentMin(String line) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_FinerSegment(line);
    }

    /**
     * 对文件中的内容进行分词
     * @param sourceFile 源文件
     * @param destFile 目标文件
     * @param isTag 是否标注词性
     * @return 是否成功
     */
    public static boolean segment(String sourceFile, String destFile, boolean isTag) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_FileProcess(sourceFile, destFile, isTag ? 1 : 0) > 0;
    }

    /**
     * 导入用户词典
     * @param dicFile    词典文件
     * @param isOverwrite 是否覆盖原先的用户词库
     * @return 是否成功
     */
    public static boolean importUserDic(String dicFile, boolean isOverwrite) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_ImportUserDict(dicFile, isOverwrite) == 1;
    }

    /**
     * 导入关键词黑名单（永远不输出此类词语）
     * @param fileName 文件名称
     * @return 是否
     */
    @SuppressWarnings("测试不生效")
    public static boolean importKeyBlackList(String fileName) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_ImportKeyBlackList(fileName) == 1;
    }

    /**
     * 添加用户词,默认词性为名词，如果需要指定词性 则： 词语\t词性  \t为制表位[Tab]
     * @param word 词
     * @return 是否成功
     */
    public static boolean addUserWord(String word) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_AddUserWord(word) == 1;
    }

    /**
     * 保存用户词库
     * @return 是否成功
     */
    @SuppressWarnings("测试后保存的文件没找到")
    public static boolean saveUserDic() {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_SaveTheUsrDic() == 1;
    }

    /**
     * 删除用户词
     *  <b>注意：只能删除通过{@link NLPIRUtil#addUserWord(String)}方法增加的词，
     *          不能删除通过词典文件导入的词语
     *  </b>
     * @param word 词语
     * @return 是否成功
     */
    public static boolean delUserWord(String word) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_DelUsrWord(word) > -1;
    }

    /**
     * 获取一个短语是一个词语的概率
     * @param canWord 短语
     * @return 概率
     */
    @SuppressWarnings("1、官方未有说明文档；2、返回科学计数法")
    public static double getWordProb(String canWord) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetUniProb(canWord);
    }

    /**
     * 判断短语是否包含在内置词典中
     * @param canWord 短语
     * @return 是否
     */
    public static boolean coreDicHasTheWord(String canWord) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_IsWord(canWord) > 0;
    }

    /**
     * 获取词性，只针对内置词典有效，
     * @param word 词语
     * @return 格式 词/词性/词序#/词性/词序#  如果有多个词性的话#隔开
     */
    @SuppressWarnings("只针对内置词典有效")
    public static String getWordNature(String word) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetWordPOS(word);
    }

    /**
     * 获取句子里边的关键词
     * @param line 句子
     * @param maxKeywordLimit 最大关键词量
     * @param weightOut 是否输出词语权重
     * @return 词/词性/权重/词频#词/词性/权重/词频#  or 词#词#词
     */
    public static String getKeyWords(String line, int maxKeywordLimit, boolean weightOut) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetKeyWords(line, maxKeywordLimit, weightOut);
    }

    /**
     * 获取文件中内容的关键词
     * @param file 文件
     * @param maxKeywordLimit 最大关键词量
     * @param weightOut 是否输出权重
     * @return 词/词性/权重/词频#词/词性/权重/词频#  or 词#词#词
     */
    public static String getFileKeyWords(String file, int maxKeywordLimit, boolean weightOut) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetFileKeyWords(file, maxKeywordLimit, weightOut);
    }

    /**
     * 获取新词
     * @param line 原始语句
     * @param maxLimit 最大词语
     * @param weightOut 是否输出权重
     * @return 词/词性/权重/词频#词/词性/权重/词频#  or 词#词#词
     */
    public static String getNewWords(String line, int maxLimit, boolean weightOut) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetNewWords(line, maxLimit, weightOut);
    }

    /**
     * 获取文件中新词
     * @param file         文件
     * @param maxLimit  最大词限制
     * @param weightOut  是否输出权重
     * @return 词/词性/权重/词频#词/词性/权重/词频#  or 词#词#词
     */
    public static String getFileNewWords(String file, int maxLimit, boolean weightOut) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetFileNewWords(file, maxLimit, weightOut);
    }

    /**
     * 生成短语指纹
     * @param line 短语
     * @return 指纹
     */
    public static long fingerprint(String line) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_FingerPrint(line);
    }

    /**
     * 获取最后一次出错信息
     * @return 具体内容
     */
    public static String getLastErrorMsg() {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetLastErrorMsg();
    }

    /**
     * 设置词性标注集
     * @param pm 标注集{@link PosMap}
     * @return 是否成功
     */
    public static boolean setPOSTag(PosMap pm) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_SetPOSmap(pm.val) == 1;
    }

    /**
     * 获取文本中词的数量
     * @param line 文本
     * @return 词语总数
     */
    public static int getWordCount(String line) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetParagraphProcessAWordCount(line);
    }

    /**
     * 获取英文原始词（过去时、进行时、单复数等的原词）
     * @param engWord 英文单词
     * @return 词语
     */
    public static String getEngWordOrigin(String engWord) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_GetEngWordOrign(engWord);
    }

    /**
     * 获取输入文本的词、词性以及词频统计，由大到小排序
     * @param line 语句
     * @return 结果 词/词性/词频#词/词性/词频#词/词性/词频...
     */
    public static String getWordFreqCount(String line) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_WordFreqStat(line);
    }

    /**
     * 获取制定文件的词、词性以及词频统计，由大到小排序
     * @param file 文件
     * @return 结果 词/词性/词频#词/词性/词频#词/词性/词频...
     */
    public static String getFileWordFreqCount(String file) {
        if(isInit()) init(UTF8_CODE);
        return instance.NLPIR_FileWordFreqStat(file);
    }

    /**
     * 可能有线程安全问题，待验证
     */
//    public static boolean getNewWordsOffline(){
//        if(instance.NLPIR_NWI_Start() == 1){
////            instance.NN
//        }
//        return true;
//    }

    /**
     * 退出分词器 (谨慎使用)
     *  执行此方法后，仍需要分词，请先执行{@link NLPIRUtil#init(EnCode)}方法才可
     * @return 是否
     */
    public synchronized static boolean exit() {
        if(isInit()) {
            isInit = false;
            return instance.NLPIR_Exit();
        }
        return true;
    }

    /**
     * 加载分词器 库文件
     */
    private static void buildInstance() {
        Properties prop = System.getProperties();
        NLPIR_DATA_PATH = NLPIRUtil.class.getResource("/nlpir").getPath();
        String libPath = NLPIR_DATA_PATH + separator + "lib" + separator, libName;
        String osName = prop.getProperty(OS_NAME);
        if(osName.toLowerCase().contains(OS_WIN_PREFIX)) {
            NLPIR_DATA_PATH = NLPIR_DATA_PATH.substring(1);
            libPath = libPath.substring(1);
            libPath += OS_WIN_PREFIX;
            libName = NLPIR_LIB_WIN;
        } else {
            libPath += OS_LINUX;
            libName = NLPIR_LIB_LINUX;
        }
        String osArch = prop.getProperty(OS_ARCH);
        if(osArch.contains(OS_ARCH_64)) {
            libPath += OS_ARCH_64;
        }else{
            libPath += OS_ARCH_32;
        }
        instance = (CLibrary) Native.loadLibrary(libPath + separator + libName, CLibrary.class);
    }
    private interface CLibrary extends Library{

        /**
         * 是否初始化
         * @return 是否
         */
        boolean NLPIR_Init();

        /**
         *
         * @param dataPath data 的父目录
         * @param encode 默认编码 0
         * @param licenceCode 公共用户忽略， 默认0
         * @return 成功或失败
         */
        int NLPIR_Init(String dataPath, int encode, int licenceCode);

        /**
         * 退出分词器
         * @return 是否
         */
        boolean NLPIR_Exit();

        /**
         * 分词
         * @param sentence 原始语句
         * @param tagging 是否标注词性 默认1 标注 ，  0 不标注
         * @return 词/n 词/t 。/w
         */
        String NLPIR_ParagraphProcess(String sentence, int tagging);

        /**
         * 获取最后一次出错错误信息
         * @return 错误信息
         */
        String NLPIR_GetLastErrorMsg();

        @Getter @Setter
        public class Result extends Structure{
            public int start;
            public int length;
            public byte[] sPos = new byte[40];
            public int iPos;
            public int wordID;
            public int wordType;
            public int weight;
            public Result(){}
            public Result(int start, int length, byte sPOS[], int iPOS,
                          int wordID, int wordType, int weight){
                super();
                this.start = start;
                this.length = length;
                this.sPos = sPOS;
                this.iPos = iPOS;
                this.wordID = wordID;
                this.wordType = wordType;
                this.weight = weight;
            }

            protected List getFieldOrder() {
                return Arrays.asList("start", "length", "sPOS", "iPOS",
                        "wordID", "wordType", "weight");
            }
            public static class ByReference extends Result implements
                    Structure.ByReference {
            };

            public static class ByValue extends Result implements
                    Structure.ByValue {
            };
        }
        /**
         * 词元
         * @param sentence 句子
         * @param resultCount pointer to result vector size
         * @param useUserDic 是否使用用户词典，默认为true
         * @return 词元
         */
        Result NLPIR_ParagraphProcessA(String sentence, int resultCount, boolean useUserDic);

        void NLPIR_ParagraphProcessAW(int nCount, Result result);

        /**
         * 统计词元数量
         * @param sentence 原始语句
         * @return 总量
         */
        int NLPIR_GetParagraphProcessAWordCount(String sentence);

        /**
         * 文本文件分词
         * @param sourceFilename 源文件
         * @param resultFilename 分词结果文件
         * @param tagging 标注
         * @return 是否成功
         */
        double NLPIR_FileProcess(String sourceFilename, String resultFilename,int tagging);

        /**
         * 导入用户词库
         * @param fileName 词库文件
         * @param overwrite 是否覆盖原有词库，默认为true
         * @return 导入成功词的总量
         */
        int NLPIR_ImportUserDict(String fileName, boolean overwrite);

        /**
         * 导入关键词黑名单
         * @param fileName 文件名称
         * @return 导入成功量
         */
        int NLPIR_ImportKeyBlackList(String fileName);

        /**
         * 添加用户词典
         * @param word 词语
         * @return 是否成功 1成功
         */
        int NLPIR_AddUserWord(String word);

        /**
         * 保存用户词典到文件
         * @return 1 成功
         */
        int NLPIR_SaveTheUsrDic();

        /**
         * 删除用户词
         * @param word 用户词
         * @return -1 词不存在； 1 删除成功
         */
        int NLPIR_DelUsrWord(String word);

        /**
         * 判断是一个词的概率
         * @param word 词
         * @return 概率
         */
        double NLPIR_GetUniProb(String word);

        /**
         * 是否是一个词（是否包含在核心词典中）
         * @param word 词
         * @return 是否
         */
        int NLPIR_IsWord(String word);

        /**
         * 获取词性
         * @param word 词语
         * @return 词性
         */
        String NLPIR_GetWordPOS(String word);

        /**
         * 获取关键词
         * @param line 原始句子
         * @param maxKeyLimit 最大关键词量 默认50
         * @param weightOut 是否输出权重（默认false）
         * @return 科学发展观 宏观经济 " or 科学发展观/23.80/12#宏观经济/12.20/1" with weight(信息熵加上词频信息)
         */
        String NLPIR_GetKeyWords(String line,int maxKeyLimit,boolean weightOut);

        /**
         * 获取关键词
         * @param fileName 文件
         * @param maxKeyLimit 最大关键词量 默认50
         * @param weightOut 是否输出权重（默认false）
         * @return 科学发展观 宏观经济 " or 科学发展观/23.80/12#宏观经济/12.20/1" with weight(信息熵加上词频信息)
         */
        String NLPIR_GetFileKeyWords(String fileName, int maxKeyLimit, boolean weightOut);

        /**
         * 提取新词
         * @param line 原始句子
         * @param maxKeyLimit 关键词最大数量
         * @param weightOut 是否输出权重
         * @return 科学发展观 屌丝 "or "科学发展观 23.80 屌丝 12.20" with weight
         */
        String NLPIR_GetNewWords(String line, int maxKeyLimit, boolean weightOut);

        /**
         * 提取新词
         * @param filename 文件
         * @param maxKeyLimit 关键词最大数量
         * @param weightOut 是否输出权重
         * @return 科学发展观 屌丝 "or "科学发展观 23.80 屌丝 12.20" with weight
         */
        String NLPIR_GetFileNewWords(String filename, int maxKeyLimit, boolean weightOut);

        /**
         * 提取指纹
         * @param line 原句
         * @return 指纹信息， 0 为失败
         */
        long NLPIR_FingerPrint(String line);

        /**
         * 设置词性标注集
         * @param nPOSmap 词性标注集
         * @return  是否成功
         */
        int NLPIR_SetPOSmap(int nPOSmap);

        /**
         * 将词语以最小粒度切分， 类似于索引分词，
         * @param line 句子
         * @return 如果不能小粒度切分 则返回空串
         */
        String NLPIR_FinerSegment(String line);

        /**
         * 获取英文单词的原型（支持单复数  过去时等）
         * @param enWord 英文词语
         * @return 结果
         */
        String NLPIR_GetEngWordOrign(String enWord);

        /**
         * 获取输入文本的词、词性以及词频统计，由大到小排序
         * @param txt 输入文本
         * @return 结果 eg.张华平/nr/10#博士/n/9#分词/n/8
         */
        String NLPIR_WordFreqStat(String txt);
        /**
         * 获取输入文本的词、词性以及词频统计，由大到小排序
         * @param fileName 文件
         * @return 结果 eg.张华平/nr/10#博士/n/9#分词/n/8
         */
        String NLPIR_FileWordFreqStat(String fileName);

        /********************************
         以下函数为2013版本专门针对新词发现的过程，一般建议脱机实现，不宜在线处理
           新词识别完成后，再自动导入到分词系统中，即可完成
           函数以NLPIR_NWI(New Word Identification)开头
        ********************************/
        /**
         * 启动新词识别系统
         * @return 是否成功
         */
        int NLPIR_NWI_Start();

        /**
         * 往新词系统加入文件
         * @param filename 文件
         * @return 是否成功
         */
        int  NLPIR_NWI_AddFile(String filename);

        /**
         * 往新词系统中加入一段待识别的文本文本
         * @param txt 文本
         * @return 是否成功
         */
        int NLPIR_NWI_AddMem(String txt);

        /**
         * 新词识别结束
         * @return ，是否成功
         */
        int NLPIR_NWI_Complete();

        /**
         * 获取新词学习结果
         * @param weightOut 是否输出权重 默认false
         * @return 【新词1】 【权重1】 【新词2】 【权重2】 ...
         */
        String NLPIR_NWI_GetResult(boolean weightOut);

        /**
         * 新词识别结果转为用户词典,返回新词结果数目
         * @return 词数
         */
        int  NLPIR_NWI_Result2UserDict();
    }
}
