<?php
namespace now;

use now\err as err;
use \PDO as PDO;

/**
 * 全局的数据操作接口
 * 请不要直接实例化此类，此类是供dao.php使用的
 * @author      欧远宁
 * @version     1.0.0
 * @copyright   CopyRight By 欧远宁
 * @package     now
 */
final class mysql {
    
    /**
     * 所有mysql的连接实例
     * @var array
     */
    private static $ins = array();

    /**
     * 数据库连接
     * @var class
     */
    public $link = null;
    
    /**
     * 是否有打开事务
     * @var boolean
     */
    public $begin = false;
    
    /**
     * dsn字符串
     * @var string
     */
    private $dsn = '';
    
    /**
     * 数据库用户名
     * @var string
     */
    private $user = '';
    
    /**
     * 数据库密码
     * @var string
     */
    private $pwd = '';

    /**
     * 构造函数，进行一些初始化
     * @author 欧远宁
     * @param string $mdl 模块名称
     * @param string $suf 分表标识
     */
    private function __construct($mdl, $suf) {
        $cfg = $GLOBALS['cfg']['db'][$mdl]['mysql'];

        $this->dsn = $cfg['dsn'];
        $this->user = $cfg['user'];
        $this->pwd = $cfg['pwd'];
    }

    /**
     * 析构函数，释放资源
     * @author 欧远宁
     */
    public function __destruct(){
        //do nothing
    }

    /**
     * 获取一个Db类实例
     * @author 欧远宁
     * @param string $mdl 模块名称
     * @param string $suf 库后缀
     * @return  一个mysql对象实例
     */
    public static function get_ins($mdl, $suf){
        $tmp = $mdl.$suf;
        if (!key_exists($tmp, self::$ins)){
            self::$ins[$tmp] = new mysql($mdl, $suf);
        }
        return self::$ins[$tmp];
    }

    /**
     * 提交所有事务
     * @author 欧远宁
     */
    public static function commit(){
        try{
            foreach(self::$ins as $db){
                if ($db->begin){
                    $db->link->commit();
                    $db->link = null;
                    $db->begin = false;
                }
            }
            self::$ins = array();
        } catch(Exception $e){
            self::$ins = array();
            throw new err($e->getMessage(),100);
        }
    }

    /**
     * 回滚所有事务
     * @author 欧远宁
     */
    public static function rollback(){
        try{
            foreach(self::$ins as $db){
                if ($db->begin){
                    $db->link->rollBack();
                    $db->link = null;
                    $db->begin = false;
                }
            }
            self::$ins = array();
        } catch(Exception $e){
            self::$ins = array();
            throw new now/err($e->getMessage(), 100);
        }
    }

    /**
     * 打开一个数据库连接
     * @author 欧远宁
     */
    private function open(){
        if (is_null($this->link)){
            try {
                $this->link = new PDO($this->dsn, $this->user, $this->pwd);
                $this->link->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);//设置为打开异常反馈
                $this->link->exec("SET NAMES utf8");//使用utf8编码
            } catch(Exception $e){
                throw new err($e->getMessage(),100);
            }
        }
    }

    /**
     * 打开事务
     * @author 欧远宁
     */
    private function begin_trans(){
        if (!$this->begin){
            try{
                $this->link->beginTransaction();
                $this->begin = TRUE;
            } catch(Exception $e){
                throw new err($e->getMessage(),100);
            }
        }
    }

    /**
     * 将一个普通sql转换成根据分页参数得到的sql
     * @author 欧远宁
     * @param string $sql 普通的含参数的sql
     * @param array $page 分页参数，格式为:array('cur'=>2, 'size'=>20, 'all'=>'y', 'start'=>10)
     * @return string
     */
    private function pageSql($sql, $page=null){
        if (!is_array($page)) {
            return $sql. ' LIMIT 1';
        }

        if (!key_exists('size', $page) || $page['size'] <= 0) {
            if(!key_exists('all', $page) || $page['all'] != 'y'){
                return $sql.' LIMIT 1';
            } else {
                return $sql;
            }
        } else {
            if (!isset($page['cur'])){
                $page['cur'] = 1;
            }
            $page['size'] = ($page['size'] > 1000) ? 1000 : $page['size'];
            $from = isset($page['start']) ? $page['start'] : ($page['cur'] - 1) * $page['size'];
            $sql .= ' LIMIT '.$from.' , '.$page['size'];
            return $sql;
        }
    }

    /**
     * 获取一个uuid
     * @author 欧远宁
     * @return string uuid的值
     */
    public function uuid(){
        $sql = 'SELECT uuid() as uuid';
        $res = $this->query($sql);
        return $res[0]['uuid'];
    }

    /**
     * 进行一次sql查询
     * @author 欧远宁
     * @param string $sql  含参数的sql语句
     * @param array $para  参数及其对应的值
     * @param array $page  分页参数，格式为:array('cur'=>2,'size'=>20,'all'=>'y')
     * @return array 结果集
     */
    public function query($sql, $para=null, $page=null) {
        $this->open();
        $result = null;
        $sql = $this->pageSql($sql, $page);    //获取分页SQL
        $this->log_sql($sql,$para);
        
        try{
            $smt = $this->link->prepare($sql);
            $smt->execute($para);
            $result =  $smt->fetchAll(PDO::FETCH_ASSOC);
            $smt = null;
        }catch(Exception $e){
            throw new err('sql error='.$e->getMessage().'. sql='.$sql,100);
        }
        return $result;
    }

    /**
     * 执行一条sql操作，并返回操作成功数
     * @author 欧远宁
     * @param string $sql 含参数的sql
     * @param array $para 参数及其对应的值
     * @return int        操作成功数
     */
    public function execute($sql, $para=null){
        $this->open();
        $re = 0;
        $this->log_sql($sql,$para);
        try{
            $this->begin_trans();
            $smt = $this->link->prepare($sql);
            $smt->execute($para);
            $re = $smt->rowCount();
            $smt = null;
        }catch(Exception $e){
            throw new err('sql error='.$e->getMessage().'. sql='.$sql,100);
        }
        return $re;
    }

    /**
     * 根据需要打印执行过的sql语句
     * @author 欧远宁
     * @param string $sql
     * @param array $param
     */
    private function log_sql($sql,$param){
        if ($GLOBALS['cfg']['cfg']['show_sql']){
            if (is_array($param) && count($param) >0){
                foreach($param as $k=>$v){
                    $sql = str_replace($k, "'$v'", $sql);
                }
                echo $sql,'<br/>';
            } else {
                echo $sql,'<br/>';
            }
        }
    }
}