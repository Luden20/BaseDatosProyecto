package crud;

import java.io.File;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.filechooser.FileNameExtensionFilter;
public class Utilidades {    
    public static boolean ValidarLongitud(String str,int max)
    {
        return str.length()>0&&str.length()<max;
    }
    public static DefaultComboBoxModel<String> ModeloCant(int i)
    {
        String[] elementos = new String [i];
        for (int x = 0; x <i; x++)
        {
            elementos[x] = String.valueOf(x+1);
        }
        return new DefaultComboBoxModel<>(elementos);
    }
    public static String rellenarEspacios(String str, int longitud) 
    {
      if(str.length()!=longitud)
      {
          for (int i =str.length();i < longitud;i++) 
        {
            str =str+ " ";
        }   
          System.out.println("STRIG TIENE "+str.length());
      }
      else
      {
          System.out.println("no se necesitan mas espacios");
      }
        return str;
    }
      public static String rellenarEspacios(String str, int longitud,String Char) 
    {
      if(str.length()!=longitud)
      {
          for (int i =str.length();i < longitud;i++) 
        {
            str =str+ Char;
        }      
      }
      else
      {
          System.out.println("no se necesitan mas espacios");
      }
        return str;
    }
    public static File obtenerArchivo(String extension,String Mensaje)
    {
        JFileChooser jFileChooser3 = new JFileChooser();
            File file=null;
            jFileChooser3.setDialogTitle(Mensaje);
            jFileChooser3.setAcceptAllFileFilterUsed(false); 
            FileNameExtensionFilter filter = new FileNameExtensionFilter("Archivos de datos(*."+extension+")", extension);
            jFileChooser3.addChoosableFileFilter(filter); 
            if (jFileChooser3.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
                file = jFileChooser3.getSelectedFile();
                String fileName = file.getName();
                 if (!fileName.toLowerCase().endsWith("." + extension.toLowerCase())) 
                 {
                    fileName += "." + extension;
                }
                file= new File(file.getParent(),fileName);
                JOptionPane.showMessageDialog(null, "Operacion exitoso", "Mensaje", JOptionPane.INFORMATION_MESSAGE);
            }
            return file;
    }
    public static File obtenerArchivo(String Mensaje)
    {
        JFileChooser jFileChooser3 = new JFileChooser();
            File file=null;
            jFileChooser3.setDialogTitle(Mensaje);
            jFileChooser3.setAcceptAllFileFilterUsed(false); 
            if (jFileChooser3.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
                file = jFileChooser3.getSelectedFile();
            }
            return file;
    }
    public static String PasswordToString(JPasswordField PASSWORD)
    {
        return new String(PASSWORD.getPassword());
    }
    
}
