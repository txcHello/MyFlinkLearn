package bigdata.state;

/**
 * @Author Administrator
 * @Date 2022/7/4 15:53
 * @Version 1.0
 * Desc:
 */
public class WC {

    public WC(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String  word;
    public  int count;
    public String getWord(){
        return   word;
    }
}
