/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package crud;

import java.sql.CallableStatement;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.PreparedStatement;
import java.util.LinkedList;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;
import java.sql.Types;

/**
 *
 * @author polip
 */
    public class ConexionOracle {
    private Connection Conexion;
    private String url;
    private String user;
    private String pass;
    
    public ConexionOracle(String IP,String SID,String User,String Password)
    {
        conectar(IP,SID,User,Password);
    }
    public void conectar(String IP,String SID,String User,String Password)
    {
        try
        {
            //De momento me coneecto como el dba, pero debo crear un rol especifico con un user especifico que maneja las tablas de la manera correcta
            Class.forName("oracle.jdbc.OracleDriver");
            url="jdbc:oracle:thin:@"+IP+":1521:"+SID;
            user=User;
            pass=Password;
            Conexion=DriverManager.getConnection(url, user, pass);
            Conexion.setAutoCommit(false);
            System.out.println("Conectado");
        }
        catch(ClassNotFoundException | SQLException e)
        {
             System.out.println("NO Conectado");
        }
    }
    
        public ResultSet ejecutarQuery(String sql) 
    {
        System.out.println(sql);
        ResultSet rs = null;
        try {
            PreparedStatement pstmt = Conexion.prepareStatement(sql);
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            
        }
        return rs;
    }
    public void desconectar()
    {
        try
        {
            Conexion.close();
            System.out.println("Desconectado");
        }
        catch(SQLException e)
        {
             System.out.println("NO Conectado");
        }
    }
    public boolean Instruccion(String sql)
    {
        try
        {
            System.out.println(sql);
            PreparedStatement pstmt = Conexion.prepareStatement(sql);
            pstmt.executeUpdate();
            return true;
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
            return false;
        }
    }
    public boolean Instruccion(String sql,String MensajeAprobatorio)
    {
        try
        {
            System.out.println(sql);
            PreparedStatement pstmt = Conexion.prepareStatement(sql);
            pstmt.executeUpdate();
            System.out.println("Se hizo bien");
            JOptionPane.showMessageDialog(null,MensajeAprobatorio, "Información", JOptionPane.INFORMATION_MESSAGE);
            return true;
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
            JOptionPane.showMessageDialog(null, "Ocurrió un error en la operación: \n"+e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
    }
    public static void printResultSet(ResultSet rs) {
        try
        {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i)+"\t");
            }
            System.out.println();         
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
        catch(SQLException e)
        {
        }
    }
    public DefaultComboBoxModel Listado(String Tabla,String Atributo)
    {
        LinkedList<String> aux=new LinkedList<String>();
        System.out.println("SELECT "+Atributo+" FROM "+Tabla+" GROUP BY "+Atributo+"");
        aux.add("VACIO");
        try
        {
            aux.clear();
            PreparedStatement p=Conexion.prepareStatement("SELECT "+Atributo+" FROM "+Tabla+" GROUP BY "+Atributo+"");
            ResultSet rs=p.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    aux.add(rs.getString(i));
                }
            };
           
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
        }
        return new DefaultComboBoxModel(aux.toArray());
    }
    public DefaultComboBoxModel ListadoComplejo(String SQL)
    {
        LinkedList<String> aux=new LinkedList<String>();
        aux.add("VACIO");
        System.out.println(SQL);
        try
        {
            aux.clear();
            PreparedStatement p=Conexion.prepareStatement(SQL);
            ResultSet rs=p.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    aux.add(rs.getString(i));
                }
            };
           
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
        }
        return new DefaultComboBoxModel(aux.toArray());
    }
    public DefaultComboBoxModel ListadoAtributos(String sql)
    {
        LinkedList<String> aux=new LinkedList<String>();
        aux.add("VACIO");
        try
        {
            aux.clear();
            PreparedStatement p=Conexion.prepareStatement(sql+" LIMIT 1;");
            ResultSet rs=p.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                aux.add(metaData.getColumnName(i));
            }
           
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
        }
        return new DefaultComboBoxModel(aux.toArray());
    }
    public int getID(String sql)
    {
        //Esta funcion asume que el query de seleccion esta bien hecho y devuelve un  solo registro con un solo atriubto
        //HAGANLO BIEN O DEVOLVERLA CUALQUIER COSA
        try
        {
            PreparedStatement p=Conexion.prepareStatement(sql);
            ResultSet rs=p.executeQuery();
            rs.next();
            printResultSet(rs);
            return rs.getInt(1);
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
            return 0;
        }
    }
    public String get(String sql)
    {
        //Esta funcion asume que el query de seleccion esta bien hecho y devuelve un  solo registro con un solo atriubto
        //HAGANLO BIEN O DEVOLVERLA CUALQUIER COSA
        try
        {
            PreparedStatement p=Conexion.prepareStatement(sql);
            ResultSet rs=p.executeQuery();
            rs.next();
            //printResultSet(rs);
            String x=rs.getString(1);
            System.out.println(x);
            return x;
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
            return "NULL";
        }
    }
    public String get2(String sql)
    {
        //Esta funcion asume que el query de seleccion esta bien hecho y devuelve un  solo registro con un solo atriubto
        //HAGANLO BIEN O DEVOLVERLA CUALQUIER COSA
        try
        {
            PreparedStatement p=Conexion.prepareStatement(sql);
            ResultSet rs=p.executeQuery();
            rs.next();
            //printResultSet(rs);
            String x=rs.getString(1);
            System.out.println(x);
            return x;
        }
        catch(SQLException e)
        {
            System.out.println(e.getMessage());
            return "NULL";
        }
    }
    public int getInt(String SQL)
    {
        return Integer.parseInt(get(SQL));
    }
    public void MostrarTabla(String query,DefaultTableModel T) {
        try
        {
            ResultSet rs=ejecutarQuery(query);
            if(rs!=null)
            {
                T.setRowCount(0);
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                LinkedList<String> aux=new LinkedList<String>();
                for (int i = 1; i <= columnCount; i++) {
                    aux.add(metaData.getColumnName(i));
                }
                T.setColumnIdentifiers(aux.toArray());
                System.out.println(); 
                LinkedList<String> aux2=new LinkedList<String>();
                while (rs.next()) {
                    aux2.clear();
                    System.out.println("-------------------");
                    for (int i = 1; i <= columnCount; i++) {
                        String saux=rs.getString(i);
                        System.out.println(saux+" "+i);
                        aux2.add(saux);
                    }
                    T.addRow(aux2.toArray());
                }
            }
            else
            {
                T.setRowCount(0);
                JOptionPane.showMessageDialog(null, "Tabla vacia", "Error", JOptionPane.ERROR_MESSAGE);

            }
            
        }
        catch(SQLException e)
        {
            T.setRowCount(0);
            JOptionPane.showMessageDialog(null, "Ocurrió un error en la operación: "+e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);

        }
    }
    public boolean FuncionAnular(int valor)
    {
        try
        {
            CallableStatement fun=Conexion.prepareCall("{ ? = call ANULAR_FACTURA(?) }");
            fun.registerOutParameter(1, Types.BOOLEAN);
            fun.setInt(2, valor); 
            fun.execute();
            JOptionPane.showMessageDialog(null, "Operacion ejecutada correctamente", "Información", JOptionPane.INFORMATION_MESSAGE);
            return fun.getBoolean(1);
        }
        catch(SQLException e)
        {
            JOptionPane.showMessageDialog(null, "Ocurrió un error en la operación: "+e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }
    }
    public double FuncionObtenerPrecio(String prd)
    {
        try
        {
            CallableStatement fun = Conexion.prepareCall("{ ? = call CAL_PREC_PRD(?) }");
            
            // Registrar el parámetro de salida (NUMBER)
            fun.registerOutParameter(1, Types.NUMERIC);
            
            // Establecer el parámetro de entrada
            fun.setString(2, prd); // Suponiendo que "P001" es el código del producto
            
            // Ejecutar la llamada
            fun.execute();
            
            // Obtener el resultado
            return  fun.getDouble(1);
        }
        catch(SQLException e)
        {
            return 0;
        }
    }
    public double FuncionIVATotal(Integer fac)
    {
        try
        {
            CallableStatement fun = Conexion.prepareCall("{ ? = call TotalIVA(?) }");
            fun.registerOutParameter(1, Types.NUMERIC);
            fun.setInt(2, fac);
            fun.execute();
            return  fun.getDouble(1);
        }
        catch(SQLException e)
        {
            return 0;
        }
    }
     public double FuncionTotalSinIVA(Integer fac)
    {
        try
        {
            CallableStatement fun = Conexion.prepareCall("{ ? = call TotalSinIva(?) }");
            fun.registerOutParameter(1, Types.NUMERIC);
            fun.setInt(2, fac);
            fun.execute();
            return  fun.getDouble(1);
        }
        catch(SQLException e)
        {
            return 0;
        }
    }
    public void ProcedimientoComision(String CedulaVen,Integer fac)
    {
        try
        {
            CallableStatement fun = Conexion.prepareCall("{call PAGAR_COMISION(?, ?)}");
            fun.setString(1,CedulaVen);
            fun.setInt(2, fac);
            fun.execute();
        }
        catch(SQLException e)
        {
           
        }
    }
    public void MostrarTablaBotn(String query,DefaultTableModel T) {
        try
        {
            ResultSet rs=ejecutarQuery(query);
            if(rs!=null)
            {
                T.setRowCount(0);
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                LinkedList<String> aux=new LinkedList<String>();
                for (int i = 1; i <= columnCount; i++) {
                    aux.add(metaData.getColumnName(i));
                }
                T.setColumnIdentifiers(aux.toArray());
                System.out.println(); 
                LinkedList<String> aux2=new LinkedList<String>();
                while (rs.next()) {
                    aux2.clear();
                    System.out.println("-------------------");
                    for (int i = 1; i <= columnCount; i++) {
                        String saux=rs.getString(i);
                        System.out.println(saux+" "+i);
                        aux2.add(saux);
                    }
                    T.addRow(aux2.toArray());
                }
            }
            else
            {
                T.setRowCount(0);
                JOptionPane.showMessageDialog(null, "Tabla vacia", "Error", JOptionPane.ERROR_MESSAGE);

            }
            
        }
        catch(SQLException e)
        {
            T.setRowCount(0);
            JOptionPane.showMessageDialog(null, "Ocurrió un error en la operación: "+e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);

        }
    }
    public void COMMIT()
    {
        try{
            Conexion.commit();
        }
        catch(SQLException e)
        {
        
        }
    }
    public void ROLLBACK()
    {
        try{
            Conexion.rollback();
        }
        catch(SQLException e)
        {
        
        }
    }
}
