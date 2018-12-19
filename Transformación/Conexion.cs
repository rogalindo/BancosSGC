using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace Transformación
{
    public class Conexion
    {

        public SqlConnection ConexionA = new SqlConnection("data source = 192.168.101.9; initial catalog = SapiensNet2; user id = sapiensnet; password = sapiensnet");
        System.Data.SqlClient.SqlCommand cmdA = new System.Data.SqlClient.SqlCommand();

        public SqlConnection AbrirConexionA()
        {
            if (ConexionA.State == ConnectionState.Closed)
                ConexionA.Open();
            cmdA.Connection = ConexionA;
            return ConexionA;
        }

        public SqlConnection CerrarConexionA()
        {
            if (ConexionA.State == ConnectionState.Open)
                ConexionA.Close();
            cmdA.Connection = ConexionA;
            return ConexionA;
        }

        //private SqlConnection ConexionT = new SqlConnection("data source = .; initial catalog = 06SapiensNet2AQP; user id = sapiensnet; password = sapiensnet");
        public SqlConnection ConexionT = new SqlConnection("data source = 192.168.101.8; initial catalog = SapiensNet; user id = sapiensnet; password = sapiensnet");
        System.Data.SqlClient.SqlCommand cmdT = new System.Data.SqlClient.SqlCommand();

        public SqlConnection AbrirConexionT()
        {
            if (ConexionT.State == ConnectionState.Closed)
                ConexionT.Open();
            cmdT.Connection = ConexionT;
            return ConexionT;
        }

        public SqlConnection CerrarConexionT()
        {
            if (ConexionT.State == ConnectionState.Open)
                ConexionT.Close();
            cmdT.Connection = ConexionT;
            return ConexionT;
        }
        //conexion para la base de datos 117 para poder guardar yt hacer la comparacion de los datos cuando se quiera hacer una comapracion
        //private SqlConnection Conexion117 = new SqlConnection("data source = .; initial catalog = 06SapiensNet2AQP; user id = sapiensnet; password = sapiensnet");
        //public SqlConnection Conexion117 = new SqlConnection("data source = 192.168.101.117; initial catalog = 06SapiensNet2AQP; user id = sapiensnet; password = sapiensnet");
        public SqlConnection Conexion117 = new SqlConnection("Data Source = localhost; Initial Catalog = 06SapiensNet2AQP; Integrated Security = True");
        System.Data.SqlClient.SqlCommand cmd117 = new System.Data.SqlClient.SqlCommand();

        public SqlConnection AbrirConexion117()
        {
            if (Conexion117.State == ConnectionState.Closed)
                Conexion117.Open();
            //cmd117.Connection = Conexion117;
            return Conexion117;
        }

        public SqlConnection CerrarConexion117()
        {
            if (Conexion117.State == ConnectionState.Open)
                Conexion117.Close();
            //cmd117.Connection = Conexion117;
            return Conexion117;
        }
    }
}
