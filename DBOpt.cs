using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SQLite;
using System.Data.SqlClient;

namespace SqliteDemo   //此处填写自己的命名空间 或者 在要调用这个类的界面中加入using DBOpt                     (c)王晋东设计及实现。
{
    //======通用C#数据库连接类，调用方法：
    //（sqlite）DBOpt.SqliteObj.(要调用的函数名称)  ||
    //（sqlserver）DBOpt.SqlserverObj.(要调用的函数名称)======
    public abstract class DBOpt
    {
        //此处填写自己的数据库连接字符串
        public static string ConnStr = @"Data Source=DataBases/test2.db";
        #region 实例化这个类的操作对象
        private static DBOpt dBOpt = null;
        //实例化这个类：sqlite调用则用下面的对象
        public static DBOpt SqliteObj
        {
            get
            {
                if (dBOpt == null)
                {
                    dBOpt = new SqliteOperate();
                }
                return dBOpt;
            }
        }
        //实例化这个类：sql server调用则用下面的对象
        public static DBOpt SqlServerObj
        {
            get
            {
                if (dBOpt == null)
                {
                    dBOpt = new SqlserverOperate();
                }
                return dBOpt;
            }
        }
        #endregion

        /// <summary>
        /// 判断是否存在
        /// </summary>
        /// <param name="sql">sql直接写语句，如“select count(*) from tableA”,下同</param>
        /// <returns>存在返回true，不存在返回false</returns>
        public abstract bool IsExist(string sql);

        /// <summary>
        /// 执行sql语句，返回一个DataTable（这个常用来给DataGridView赋值）
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns>如果不存在返回null</returns>
        public abstract DataTable GetDataTable(string sql);

        /// <summary>
        /// 执行插入语句
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns>返回成功的行数，如果没插入成功则是0</returns>
        public abstract int Insert(string sql);

        /// <summary>
        /// 执行删除语句
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns>返回删除的行数，如果没删则是0</returns>
        public abstract int Delete(string sql);

        /// <summary>
        /// 执行更新的语句
        /// </summary>
        /// <param name="sql">要执行的sql语句</param>
        /// <returns>返回更新的行数，如果没更新则是0</returns>
        public abstract int Update(string sql);

        /// <summary>
        /// 执行一条查询，返回查询结果（object类型）。(在使用时要把object强制转换成自己要的类型)
        /// </summary>
        /// <param name="SQLString">查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public abstract object GetSqlResult(string sql);

        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。（如果中间有失败则会回滚，所以比较安全）
        /// </summary>
        /// <param name="SQLStringList">多条SQL语句的List String</param>		
        public abstract int ExecuteSqlTran(List<string> sqlList);
        
    }

    /*==============================下面这几个类是上面的具体实现，不用看了===========================*/

    internal class SqliteOperate : DBOpt
    {
        public override bool IsExist(string sql)
        {
            SQLiteConnection con = new SQLiteConnection(ConnStr);
            SQLiteCommand com = new SQLiteCommand(sql, con);
            try
            {
                con.Open();
                if ((int)com.ExecuteScalar() > 0)
                {
                    return true;
                }
                return false;
            }
            catch
            {
                return false;
            }
            finally
            {
                com.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public override DataTable GetDataTable(string sql)
        {
            SQLiteConnection con = new SQLiteConnection(ConnStr);
            SQLiteDataAdapter sda = new SQLiteDataAdapter(sql, con);
            try
            {
                con.Open();
                DataTable dt = new DataTable();
                sda.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
            finally
            {
                sda.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public int ExecuteSql(string sql)
        {
            SQLiteConnection con = new SQLiteConnection(ConnStr);
            SQLiteCommand com = new SQLiteCommand(sql, con);
            try
            {
                con.Open();
                int count = com.ExecuteNonQuery();
                return count;
            }
            catch
            {
                return 0;
            }
            finally
            {
                com.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public override int Insert(string sql)
        {
            return this.ExecuteSql(sql);
        }

        public override int Delete(string sql)
        {
            return this.ExecuteSql(sql);
        }

        public override int Update(string sql)
        {
            return this.ExecuteSql(sql);
        }
        
        public override object GetSqlResult(string sql)
        {
            SQLiteConnection con = new SQLiteConnection(ConnStr);
            SQLiteCommand com = new SQLiteCommand(sql, con);
            try
            {
                con.Open();
                object obj = com.ExecuteScalar();
                if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                {
                    return null;
                }
                else
                {
                    return obj;
                }
            }
            catch (SQLiteException e)
            {
                throw e;
            }
            finally
            {
                com.Dispose();
                con.Close();
                con.Dispose();
            }
        }
        
        public override int ExecuteSqlTran(List<String> SQLStringList)
        {
            SQLiteConnection con = new SQLiteConnection(ConnStr);
            SQLiteCommand com = new SQLiteCommand();
            com.Connection = con;
            con.Open();
            SQLiteTransaction st = con.BeginTransaction();
            com.Transaction = st;
            try
            {
                int count = 0;
                for (int n = 0; n < SQLStringList.Count; n++)
                {
                    string strsql = SQLStringList[n];
                    if (strsql.Trim().Length > 1)
                    {
                        com.CommandText = strsql;
                        count += com.ExecuteNonQuery();
                    }
                }
                st.Commit();
                return count;
            }
            catch
            {
                st.Rollback();
                return 0;
            }
            finally
            {
                com.Dispose();
                st.Dispose();
                con.Close();
                con.Dispose();
            }
        }
    }
    internal class SqlserverOperate : DBOpt
    {
        public override bool IsExist(string sql)
        {
            SqlConnection con = new SqlConnection(ConnStr);
            SqlCommand com = new SqlCommand(sql, con);
            try
            {
                con.Open();
                if ((int)com.ExecuteScalar() > 0)
                {
                    return true;
                }
                return false;
            }
            catch
            {
                return false;
            }
            finally
            {
                com.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public override DataTable GetDataTable(string sql)
        {
            SqlConnection con = new SqlConnection(ConnStr);
            SqlDataAdapter sda = new SqlDataAdapter(sql, con);
            try
            {
                con.Open();
                DataTable dt = new DataTable();
                sda.Fill(dt);
                return dt;
            }
            catch
            {
                return null;
            }
            finally
            {
                sda.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public int ExecuteSql(string sql)
        {
            SqlConnection con = new SqlConnection(ConnStr);
            SqlCommand com = new SqlCommand(sql, con);
            try
            {
                con.Open();
                int count = com.ExecuteNonQuery();
                return count;
            }
            catch
            {
                return 0;
            }
            finally
            {
                com.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public override int Insert(string sql)
        {
            return this.ExecuteSql(sql);
        }

        public override int Delete(string sql)
        {
            return this.ExecuteSql(sql);
        }

        public override int Update(string sql)
        {
            return this.ExecuteSql(sql);
        }

        public override object GetSqlResult(string sql)
        {
            SqlConnection con = new SqlConnection(ConnStr);
            SqlCommand com = new SqlCommand(sql, con);
            try
            {
                con.Open();
                object obj = com.ExecuteScalar();
                if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                {
                    return null;
                }
                else
                {
                    return obj;
                }
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                throw e;
            }
            finally
            {
                com.Dispose();
                con.Close();
                con.Dispose();
            }
        }

        public override int ExecuteSqlTran(List<String> SQLStringList)
        {
            SqlConnection con = new SqlConnection(ConnStr);
            SqlCommand com = new SqlCommand();
            com.Connection = con;
            con.Open();
            SqlTransaction st = con.BeginTransaction();
            com.Transaction = st;
            try
            {
                int count = 0;
                for (int n = 0; n < SQLStringList.Count; n++)
                {
                    string strsql = SQLStringList[n];
                    if (strsql.Trim().Length > 1)
                    {
                        com.CommandText = strsql;
                        count += com.ExecuteNonQuery();
                    }
                }
                st.Commit();
                return count;
            }
            catch
            {
                st.Rollback();
                return 0;
            }
            finally
            {
                com.Dispose();
                st.Dispose();
                con.Close();
                con.Dispose();
            }
        }
    }

}
