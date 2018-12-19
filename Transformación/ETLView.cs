using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.IO;
using System.Globalization;

namespace Transformación
{
    public partial class ETLView : Form
    {
        //Datos del documento txt
        String fecha_cab = "";
        Double monto_cab = 0;
        int lineas_cab = 0;

        string abonado_det = "";
        string fecha_det = "";
        string limite_det = "";
        string dinero_det = "";
        double totCal_det = 0;

        int cabecera = 0;
        int detalle = 0;
        int contador = 0;
        int pie = 0;

        string diaSem;
        string date_valid;

        /*string dia = "lunes";
        int ContLin = 0;
        int totBD = 0;*/

        //Fecha actual y del día anterior para usar con Format
        DateTime fech = DateTime.Today.AddDays(0);
        DateTime fechpas = DateTime.Today.AddDays(-1);

        //Listas para almacenar datos de cabecera y detalle del .txt
        List<string> datosCab = new List<string>();
        List<string> datosDet = new List<string>();

        //Arreglos a usar para guardar los datos
        string[] arrDatosDet;
        string[,] matDatosDet;

        //Instancias para llenar DGVBD
        SqlDataAdapter daBD;
        DataTable dtBD = new DataTable();

        //Definiciones de la base de datos  
        //public SqlConnection Conexion117 = new SqlConnection("data source = 192.168.101.117; initial catalog = 06SapiensNet2AQP; user id = sapiensnet; password = sapiensnet");
        //Dirección SQL de la conexión
        public SqlConnection Conexion117 = new SqlConnection("data source = 192.168.101.9; initial catalog = SapiensNet2; user id = sapiensnet; password = sapiensnet");
        //public SqlConnection Conexion117 = new SqlConnection("Data Source = localhost; Initial Catalog = 06SapiensNet2AQP; Integrated Security = True");
        System.Data.SqlClient.SqlCommand cmd117 = new System.Data.SqlClient.SqlCommand();

        public SqlConnection AbrirConexion117() //abrir conexión
        {
            if (Conexion117.State == ConnectionState.Closed)
                Conexion117.Open();
            cmd117.Connection = Conexion117;
            return Conexion117;
        }

        public SqlConnection CerrarConexion117() //cerrar conexión
        {
            if (Conexion117.State == ConnectionState.Open)
                Conexion117.Close();
            cmd117.Connection = Conexion117;
            return Conexion117;
        }
        //Procedimiento con archivo plano del BBVA
        public void LeerBBVA()
        {
            try
            {
                //ruta para leer el archivo de texto
                //lectura de datos especificos delimitados por una matriz
                string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\BBVA\rptaBBVA.txt");
                //string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\BBVA\{0}\{1}\{2}\Nuevo documento de texto.txt", fech.ToString("yyyy"), fech.ToString("MM"), fech.ToString("dd"));
                //string path = string.Format(@"C:\Users\arni_\OneDrive\Documentos\RJGS\Desarrollo\BBVA.txt");
                using (StreamReader sr = new StreamReader(path))
                {
                    while (sr.Peek() >= 0)
                    {
                        string linea = sr.ReadLine();
                        char tipo = Convert.ToChar(linea.Substring(1, 1));
                        switch (tipo)
                        {
                            //extraer detalle datos de la cabecera del txt ségún formato 
                            case '1':
                                cabecera++;
                                fecha_cab = Convert.ToString(linea.Substring(19, 8));
                                datosCab.Add(fecha_cab);
                                break;
                            //extraer datos del cuerpo del txt según formato
                            case '2':
                                detalle++;
                                abonado_det = Convert.ToString(linea.Substring(32, 8));
                                //abonado_det = abonado_det.TrimStart('0'); //para eliminar los 0 de la izquierda
                                datosDet.Add(abonado_det);
                                fecha_det = Convert.ToString(linea.Substring(135, 8));
                                datosDet.Add(fecha_det);
                                limite_det = Convert.ToString(linea.Substring(40, 12));
                                datosDet.Add(limite_det);
                                dinero_det = Convert.ToString(linea.Substring(80, 15));
                                totCal_det = totCal_det + Convert.ToDouble(dinero_det);
                                datosDet.Add(dinero_det);
                                break;
                            //extraer datos del pie del txt según formato
                            case '3':
                                pie++;
                                lineas_cab = Convert.ToInt32(linea.Substring(6, 5));
                                datosCab.Add(Convert.ToString(lineas_cab));
                                monto_cab = Convert.ToDouble(linea.Substring(16, 10)) / 100;
                                datosCab.Add(Convert.ToString(monto_cab));
                                break;

                            default:
                                contador++;
                                break;
                        }
                    }
                }
            }
            catch
            {
                lblError.Text = "Nombre del Archivo no coincise debe abrir Nuevo documento de texto";
            }
        }
        //Procedimiento con archivo plano del BCP
        public void LeerBCP()
        {
            try
            {
                //ruta para leer el archivo de texto
                //lectura de datos especificos delimitados por una matriz
                string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\BCP\rptaBCP.TXT");
                //string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\BCP\{0}\{1}\{2}\CDPG2225.TXT", fech.ToString("yyyy"), fech.ToString("MM"), fech.ToString("dd"));
                //string path = string.Format(@"C:\Users\arni_\OneDrive\Documentos\RJGS\Desarrollo\BCP.txt");
                using (StreamReader sr = new StreamReader(path))
                {
                    while (sr.Peek() >= 0)
                    {
                        string linea = sr.ReadLine();
                        char tipo = Convert.ToChar(linea.Substring(0, 1)); //para derivar a Case
                        switch (tipo)
                        {
                            //extraer detalle datos de la cabecera del txt según formato
                            case 'C':
                                cabecera++;
                                fecha_cab = Convert.ToString(linea.Substring(14, 8));
                                datosCab.Add(fecha_cab);
                                lineas_cab = Convert.ToInt32(linea.Substring(23, 8));
                                datosCab.Add(Convert.ToString(lineas_cab));
                                monto_cab = Convert.ToDouble(linea.Substring(32, 14)) / 100;
                                datosCab.Add(Convert.ToString(monto_cab));
                                break;
                            //extraer datos del cuerpo del txt según formato
                            case 'D':
                                detalle++;
                                abonado_det = Convert.ToString(linea.Substring(13, 14));
                                //abonado_det = abonado_det.TrimStart('0'); //para eliminar los 0 de la izquierda
                                datosDet.Add(abonado_det);
                                fecha_det = Convert.ToString(linea.Substring(57, 8));
                                datosDet.Add(fecha_det);
                                limite_det = Convert.ToString(linea.Substring(65, 8));
                                datosDet.Add(limite_det);
                                dinero_det = Convert.ToString(linea.Substring(73, 15));
                                totCal_det = totCal_det + Convert.ToDouble(dinero_det);
                                datosDet.Add(dinero_det);
                                break;

                            default:
                                contador++;
                                break;
                        }
                    }
                }
            }
            //mensaje de comprovacion error al seleccionar el archivo txt
            catch
            {
                lblError.Text = "Nombre del Archivo no coincide debe abrir CDPG2225";
            }
        }
        //Procedimiento con ambos archivos planos Interbank
        public void LeerIB()
        {
            //Archivo con los registros detallados
            try
            {
                //ruta para leer el archivo de texto
                //lectura de datos especificos delimitados por una matriz
                string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\INTERBANK\rptaIB1.TXT");
                //string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\INTERBANK\{0}\{1}\{2}\22073390118{3}.txt", fech.ToString("yyyy"), fech.ToString("MM"), fech.ToString("dd"), fechpas.ToString("MMdd"));
                //string path = string.Format(@"C:\Users\arni_\OneDrive\Documentos\RJGS\Desarrollo\IBdet.txt");
                using (StreamReader sr = new StreamReader(path))
                {
                    while (sr.Peek() >= 0)
                    {
                        string linea = sr.ReadLine();
                        char tipo = Convert.ToChar(linea.Substring(1, 1));
                        switch (tipo)
                        {
                            //extraer datos del detalle del txt según formato
                            case '7':
                                detalle++;
                                fecha_cab = Convert.ToString(linea.Substring(82, 8));
                                abonado_det = Convert.ToString(linea.Substring(9, 6));
                                datosDet.Add(abonado_det);
                                fecha_det = Convert.ToString(linea.Substring(82, 8));
                                datosDet.Add(fecha_det);
                                limite_det = Convert.ToString(linea.Substring(29, 8));
                                datosDet.Add(limite_det);
                                dinero_det = Convert.ToString(linea.Substring(96, 13));
                                totCal_det = totCal_det + Convert.ToDouble(dinero_det);
                                datosDet.Add(dinero_det);
                                break;

                            default:
                                contador++;
                                break;
                        }
                    }
                }
            }
            catch
            {
                lblError.Text = string.Format("Nombre del Archivo no coincise debe abrir sbp{0}", fechpas.ToString("MMdd"));
            }
            //Archivo con la información de la cabecera
            try
            {
                string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\INTERBANK\rptaIB2.TXT");
                //string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\INTERBANK\{0}\{1}\{2}\21073390118{3}.txt", fech.ToString("yyyy"), fech.ToString("MM"), fech.ToString("dd"), fechpas.ToString("MMdd"));
                //string path = string.Format(@"C:\Users\arni_\OneDrive\Documentos\RJGS\Desarrollo\IBres.txt");
                using (StreamReader sr1 = new StreamReader(path))
                {
                    while (sr1.Peek() >= 0)
                    {
                        string linea1 = sr1.ReadLine();
                        char tipo1 = Convert.ToChar(linea1.Substring(0, 1));
                        //string datos = linea1.Substring(1);
                        switch (tipo1)
                        {
                            //extraer detalle datos de la "cabecera" del txt ségún formato 
                            case 'S':
                                cabecera++;
                                try
                                {
                                    cabecera = 2;
                                    datosCab.Add(fecha_cab);
                                    lineas_cab = Convert.ToInt32(linea1.Substring(31, 8));
                                    datosCab.Add(Convert.ToString(lineas_cab));
                                    monto_cab = Convert.ToDouble(linea1.Substring(42, 12)) ;
                                    datosCab.Add(Convert.ToString(monto_cab));
                                }
                                catch
                                { }
                                break;
                            default:
                                contador++;
                                break;
                        }
                    }
                }
            }
            catch
            {
                lblError.Text = string.Format("Nombre del Archivo no coincise debe abrir 22073390118{0}", fech.ToString("MMdd"));
            }
        }
        //Procedimiento con archivo plano del Scotiabank
        public void LeerSB()
        {
            try
            {
                //ruta para leer el archivo de texto
                //lectura de datos especificos delimitados por una matriz
                string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\Scotiabank\rptaSB.TXT");
                //string path = string.Format(@"\\192.168.101.36\Documentos - Sistemas\Progs\Scotiabank\{0}\{1}\{2}\sbp{3}.txt", fech.ToString("yyyy"), fech.ToString("MM"), fech.ToString("dd"), fechpas.ToString("MMdd"));
                //string path = string.Format(@"C:\Users\arni_\OneDrive\Documentos\RJGS\Desarrollo\Scotia.txt");
                using (StreamReader sr = new StreamReader(path))
                {
                    while (sr.Peek() >= 0)
                    {
                        string linea = sr.ReadLine();
                        char tipo = Convert.ToChar(linea.Substring(0, 1)); //para derivar a Case
                        switch (tipo)
                        {
                            //extraer detalle datos de la cabecera del txt ségún formato 
                            case 'C':
                                cabecera++;
                                if (cabecera == 1)
                                {
                                    fecha_cab = Convert.ToString(linea.Substring(14, 8));
                                    datosCab.Add(fecha_cab);
                                    lineas_cab = Convert.ToInt32(linea.Substring(23, 8));
                                    datosCab.Add(Convert.ToString(lineas_cab));
                                    monto_cab = Convert.ToDouble(linea.Substring(32, 14))/100;
                                    datosCab.Add(Convert.ToString(monto_cab));
                                    break;
                                }
                                break;
                            //extraer datos del cuerpo del txt ségún formato 
                            case 'D':
                                detalle++;
                                abonado_det = Convert.ToString(linea.Substring(13, 14));
                                //abonado_det = abonado_det.TrimStart('0'); //para eliminar los 0 de la izquierda
                                datosDet.Add(abonado_det);
                                fecha_det = Convert.ToString(linea.Substring(57, 8));
                                datosDet.Add(fecha_det);
                                limite_det = Convert.ToString(linea.Substring(65, 8));
                                datosDet.Add(limite_det);
                                dinero_det = Convert.ToString(linea.Substring(73, 15));
                                totCal_det = totCal_det + Convert.ToDouble(dinero_det);
                                datosDet.Add(dinero_det);
                                break;

                            default:
                                contador++;
                                break;
                        }
                    }
                }
            }
            //mensaje de comprovacion error al seleccionar el archivo txt
            catch
            {
                lblError.Text = string.Format("Nombre del Archivo no coincise debe abrir sbp{0}", fechpas.ToString("MMdd"));
            }
        }

        void MBoxConfirm()
        {
            diaSem = fech.ToString("dddd", new CultureInfo("es-ES"));
            date_valid = fechpas.ToString("yyyyMMdd");
            if (MessageBox.Show(" La fecha extraída del archivo txt es: " + fecha_cab + "\n La fecha de validación es: " + date_valid + "\n Hoy día es: " + diaSem + "\n¿Desea continuar?...", "Confirmar", MessageBoxButtons.OKCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button1, MessageBoxOptions.DefaultDesktopOnly) == DialogResult.OK)
            {
                return;
            }
            else
            {
                MessageBox.Show("Se reiniciará el procedimiento, cambie el archivo por el correcto ", "Reiniciando programa...", MessageBoxButtons.OK, MessageBoxIcon.Error);
                Application.Restart();

                //return; 
            }
        }
        public void Comprobar()
        {

        }
        //Limpiamos los list y array en caso se ejecute n veces
        public void LimpiarArrList()
        {
            Array.Clear(arrDatosDet, 0, arrDatosDet.Length);
            datosDet.Clear();
            datosCab.Clear();
        }
        //Limpiar los text box
        public void Limpiar()
        {
            txtFec_cab.Clear();
            txtLin_cab.Clear();
            txtTot_cab.Clear();
            txtFilDGVTXT.Clear();
            txtTotDGVTXT.Clear();
            txtFilDGVBD.Clear();
            txtTotDGVBD.Clear();
        }
        //Limpiar data grid view izquierda
        void LimpiarDGVTXT()
        {
            if (dGVTXT.RowCount > 0)
            {
                dGVTXT.Rows.Clear();
            }
        }
        //Llenar datos de encabezado
        void LlenarDatosTXT()
        {
            if (datosCab[0]==datosCab[1]) 
            //Caso interbank
            {
                txtFec_cab.Text = datosCab[0];
                txtLin_cab.Text = datosCab[2];
                txtTot_cab.Text = "S/. " + datosCab[3];
            }
            else
            //Demás bancos
            {
                txtFec_cab.Text = datosCab[0];
                txtLin_cab.Text = datosCab[1];
                txtTot_cab.Text = "S/. " + datosCab[2];
            }
            
        }
        //Llenar data grid view izquierda
        void LlenarDatosDGVTXT()
        {
            txtFilDGVTXT.Text = Convert.ToString(dGVTXT.RowCount);
            txtTotDGVTXT.Text = "S/. "+ totCal_det /100;
        }
        //Generamos la matriz en base al arreglo de los datos del detalle
        string[,] GeneraMatriz()
        {
            //definimos una matriz nueva luego de haber limpiado el arreglo y matriz externo
            string[,] matDet;
            arrDatosDet = datosDet.ToArray();
            int k = 0;
            matDet = new string[detalle, 4];
            for (int i = 0; i < detalle; i++)
            {
                for (int j = 0; j < 4; j++)
                {
                    //pasar datos del arregloDetalle a la matrizDetalle
                    if (k >= arrDatosDet.Count())
                    {
                        break;//si ya no encuentra elementos en array
                    }
                    else
                    {
                        matDet[i, j] = arrDatosDet[k];
                        k++;
                    }
                }
            }
            return matDet; //retornamos la matriz
            
        }
        //Insertar matriz en tabla TMP_BCPR
        void InsertBD117()
        {
            cmd117.CommandType = System.Data.CommandType.Text;
            AbrirConexion117();
            for (int i = 0; i < matDatosDet.GetLength(0); i++)
            {
                cmd117.CommandText = "INSERT INTO dbo.tmp_bcpr ( COL002, COL004, COL005, COL006) VALUES ('" + matDatosDet[i, 0] + "','" + matDatosDet[i, 1] + "','" + matDatosDet[i, 2] + "','" + matDatosDet[i, 3] + "')";
                //cmd117.CommandText = "INSERT INTO dbo.test_tmp_bcpr (COL002, COL004, COL005, COL006) VALUES ('" + matDatosDet[i, 0] + "','" + matDatosDet[i, 1] + "','" + matDatosDet[i, 2] + "','" + matDatosDet[i, 3] + "')";
                cmd117.ExecuteNonQuery();
            }
            CerrarConexion117();
        }
        //Limpiar tabla TMP_BCPR
        void DeleteBD117()
        {
            cmd117.CommandType = System.Data.CommandType.Text;
            AbrirConexion117();
            cmd117.CommandText = "DELETE FROM dbo.tmp_bcpr";
            //cmd117.CommandText = "DELETE FROM dbo.test_tmp_bcpr";
            cmd117.ExecuteNonQuery();
            CerrarConexion117();
        }
        //Llenar Data Grid View de la izquierda
        void CargarDGVTXT()
        {
            for (int i = 0; i < matDatosDet.GetLength(0); i++)
            {
                string[] row = new string[matDatosDet.GetLength(1)];
                for (int j = 0; j < matDatosDet.GetLength(1); j++)
                {
                    row[j] = matDatosDet[i, j];
                }
                dGVTXT.Rows.Add(row);
            }
            dGVTXT.Sort(dGVTXT.Columns[1], ListSortDirection.Ascending);
        }
        //Llenar Data Grid View de la derecha
        void CargarDGVBD(DataGridView dgvBD)
        {
            try
            {
                daBD = new SqlDataAdapter("SELECT * FROM tmp_bcpr order by col004 asc", Conexion117);
                //daBD = new SqlDataAdapter("SELECT * FROM test_tmp_bcpr order by col004 asc", Conexion117);
                dtBD = new DataTable();
                daBD.Fill(dtBD);
                dgvBD.DataSource = dtBD;
            }
            catch (Exception ex)
            {
                MessageBox.Show("No se pudo llenar DGV" + ex.ToString());
            }
        }
        //Limpiar variables para n ejecuciones
        void ResetVars()
        {
            cabecera = 0;
            totCal_det = 0;
            detalle = 0;
        }
        //Calculamos total según col006 del data grid view derecha y actualizamos los text box con la información
        void CalctotBD117()
        {
            double sum = 0;
            foreach (DataGridViewRow row in dGVBD.Rows)
            {
                sum += Convert.ToDouble(row.Cells["col006"].Value);
            }
            sum = sum / 100;
            txtTotDGVBD.Text = "S/. " + Convert.ToString(sum);
            txtFilDGVBD.Text = Convert.ToString(dGVBD.RowCount);
        }
        public ETLView()
        {
            InitializeComponent();
        }

        private void Label3_Click(object sender, EventArgs e)
        {

        }

        private void TextBox3_TextChanged(object sender, EventArgs e)
        {

        }
        //Disparar BBVA
        private void BtnBBVA_Click(object sender, EventArgs e)
        {
            LimpiarDGVTXT();
            DeleteBD117();
            Limpiar();
            ResetVars();
            LeerBBVA();
            MBoxConfirm();
            matDatosDet = GeneraMatriz();
            CargarDGVTXT();
            LlenarDatosTXT();
            LlenarDatosDGVTXT();
            InsertBD117();
            CargarDGVBD(dGVBD);
            CalctotBD117();
            LimpiarArrList();
        }
        //Disparar BCP
        private void BtnBCP_Click(object sender, EventArgs e)
        {
            LimpiarDGVTXT();
            DeleteBD117();
            Limpiar();
            ResetVars();
            LeerBCP();
            MBoxConfirm();
            matDatosDet = GeneraMatriz();
            CargarDGVTXT();
            LlenarDatosTXT();
            LlenarDatosDGVTXT();
            InsertBD117();
            CargarDGVBD(dGVBD);
            CalctotBD117();
            LimpiarArrList();
        }
        //Disparar Interbank
        private void BtnIB_Click(object sender, EventArgs e)
        {
            LimpiarDGVTXT();
            DeleteBD117();
            Limpiar();
            ResetVars();
            LeerIB();
            MBoxConfirm();
            matDatosDet = GeneraMatriz();
            CargarDGVTXT();
            LlenarDatosTXT();
            LlenarDatosDGVTXT();
            InsertBD117();
            CargarDGVBD(dGVBD);
            CalctotBD117();
            LimpiarArrList();
        }
        //Disparar Scotiabank
        private void BtnSB_Click(object sender, EventArgs e)
        {
            LimpiarDGVTXT();
            DeleteBD117();
            Limpiar();
            ResetVars();
            LeerSB();
            MBoxConfirm();
            matDatosDet = GeneraMatriz();
            CargarDGVTXT();
            LlenarDatosTXT();
            LlenarDatosDGVTXT();
            InsertBD117();
            CargarDGVBD(dGVBD);
            CalctotBD117();
            LimpiarArrList();
        }
        //Actualizar fecha (col004) en tabla TMP_BCPR
        private void BtnActFecha_Click(object sender, EventArgs e)
        {
            cmd117.CommandType = System.Data.CommandType.Text;
            AbrirConexion117();
            cmd117.CommandText = "update dbo.tmp_bcpr set col004='" + txtNewFecha.Text + "'";
            //cmd117.CommandText = "update dbo.test_tmp_bcpr set col004='" + txtNewFecha.Text + "'";
            cmd117.ExecuteNonQuery();
            CerrarConexion117();
        }
        //Actualizamos data grid view derecha y ordenamos ambos según primer columna
        private void BtnActDGVBD_Click(object sender, EventArgs e)
        {
            CargarDGVBD(dGVBD);
            dGVBD.Sort(dGVBD.Columns[0], ListSortDirection.Ascending);
            dGVTXT.Sort(dGVTXT.Columns[0], ListSortDirection.Ascending);
            CalctotBD117();
            LlenarDatosDGVTXT();
        }
    }
}
