using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace Modulo3
{
    class Program
    {
        private DocumentClient client;
        
        public async Task RunstoredProcedure(string dataBaseName, string collectionName, Usuario usuario)
        {
            //the predefined Storage procedure just queries the database doesnt save any kind of document with the queryDocuments()
            string varu = await this.client.ExecuteStoredProcedureAsync<string>(UriFactory.CreateStoredProcedureUri(dataBaseName, collectionName, "UpdateOrderTotal"), new RequestOptions { PartitionKey = new PartitionKey(usuario.UsuarioId) });
            Console.WriteLine("Procedimiento de consulta COMPLETO.");
        }

        static void Main(string[] args)
        {
            try
            {
                //Definicion de Crud basico para datos no-SQL en base de datos CosmosDB
                Usuario nelapin = new Usuario
                {
                    Id = "2",
                    UsuarioId = "nelapin",
                    LastName = "Hpindakova",
                    FirstName = "nelapin",
                    Email = "nelapin@contoso.com",
                    OrderHistory = new OrderHistory[]
                   {
                        new OrderHistory
                        {
                            OrderId = "1001",
                            DateShipped = "08/17/2018",
                            Total = "106.9"
                        }
                    },
                    ShippingPreference = new ShippingPreference[]
                {
                    new ShippingPreference
                    {
                        Priority = 2,
                        AddressLine1 = "90 W 82th St",
                        City = "New York",
                        State = "NY",
                        ZipCode = "10001",
                        Country = "USA"
                    }
                },
                    CuponUsed = new CuponUsed[]
                {
                    new CuponUsed{ CouponCode ="Fall2018"}
                }
                };
                Usuario nelapinSP = new Usuario
                {
                    Id = "4",
                    UsuarioId = "nelapinSP",
                    LastName = "Hpindakova",
                    FirstName = "nelapin",
                    Email = "nelapin@contoso.com",
                    OrderHistory = new OrderHistory[]
                   {
                        new OrderHistory
                        {
                            OrderId = "1001",
                            DateShipped = "08/17/2018",
                            Total = "106.9"
                        }
                    },
                    ShippingPreference = new ShippingPreference[]
                {
                    new ShippingPreference
                    {
                        Priority = 2,
                        AddressLine1 = "90 W 82th St",
                        City = "New York",
                        State = "NY",
                        ZipCode = "10001",
                        Country = "USA"
                    }
                },
                    CuponUsed = new CuponUsed[]
                {
                    new CuponUsed{ CouponCode ="Fall2018"}
                }
                };
                Console.WriteLine("Hello World!");

                Program program = new Program();
                program.BasicOperation().Wait();
                program.ReadIfDocument(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, nelapin).Wait();
                program.RunstoredProcedure(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, nelapin).Wait();
                program.DeleteUserDocument(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, nelapin).Wait();
                program.ExecuteSimpleQuery(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId);
                
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error ocurred: {1}, Message:{2}",
                de.StatusCode, de.Message, baseException.Message);

            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }


        }

        /// <summary>
        /// crea una base de datos si no existe con una partitionkey
        /// </summary>
        /// <returns></returns>
        private async Task BasicOperation()
        {
            this.client = new DocumentClient(new Uri(ConfigurationManager.AppSettings["accountEndPoint"]), ConfigurationManager.AppSettings["accountKey"]);

            // check if a database exist if don´t so create the database with the Id specified
            Database db = await this.client.CreateDatabaseIfNotExistsAsync(new Database { Id = EnumDatosBasicos.DataBaseId }, new RequestOptions{PartitionKey = new PartitionKey(EnumDatosBasicos.PartitionKey)});
                       
            // check if a collection exist if don't creates the specified one.
            DocumentCollection dc = await this.client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(EnumDatosBasicos.DataBaseId), new DocumentCollection { Id = EnumDatosBasicos.CollectionId });

            Usuario yanhe = new Usuario
            {
                Id = "1",
                UsuarioId = "yanhe",
                LastName = "He",
                FirstName = "Yan",
                Email = "yanhe@contoso.com",
                OrderHistory = new OrderHistory[]
                   {
                        new OrderHistory
                        {
                            OrderId = "1000",
                            DateShipped = "08/17/2018",
                            Total = "52.49"
                        }
                    },
                ShippingPreference = new ShippingPreference[]
                {
                    new ShippingPreference
                    {
                        Priority = 1,
                        AddressLine1 = "90 W 8th St",
                        City = "New York",
                        State = "NY",
                        ZipCode = "10001",
                        Country = "USA"
                    }
                },
            };
            await this.CreateUserDocumentIfNotExist(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, yanhe);
            Usuario nelapin = new Usuario
            {
                Id = "2",
                UsuarioId = "nelapin",
                LastName = "Pindakova",
                FirstName = "nelapin",
                Email = "nelapin@contoso.com",
                OrderHistory = new OrderHistory[]
                   {
                        new OrderHistory
                        {
                            OrderId = "1001",
                            DateShipped = "08/17/2018",
                            Total = "106.9"
                        }
                    },
                ShippingPreference = new ShippingPreference[]
                {
                    new ShippingPreference
                    {
                        Priority = 2,
                        AddressLine1 = "90 W 82th St",
                        City = "New York",
                        State = "NY",
                        ZipCode = "10001",
                        Country = "USA"
                    }
                },
                CuponUsed = new CuponUsed[]
                {
                    new CuponUsed{ CouponCode ="Fall2018"}
                }
            };
            await this.CreateUserDocumentIfNotExist(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, nelapin);
            Usuario nolose = new Usuario
            {
                Id = "3",
                UsuarioId = "nolose",
                LastName = "Lose",
                FirstName = "no",
                Email = "nolose@contoso.com",
                OrderHistory = new OrderHistory[]
                   {
                        new OrderHistory
                        {
                            OrderId = "1002",
                            DateShipped = "08/18/2018",
                            Total = "10"
                        }
                    },
                ShippingPreference = new ShippingPreference[]
                {
                    new ShippingPreference
                    {
                        Priority = 1,
                        AddressLine1 = "10 E 2th La Salle St",
                        City = "Chicago",
                        State = "NY",
                        ZipCode = "20032",
                        Country = "USA"
                    }
                },
                CuponUsed = new CuponUsed[]
                {
                    new CuponUsed{ CouponCode ="Fall2018"}
                }
            };
            await this.CreateUserDocumentIfNotExist(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, nolose);
            Usuario nelapin2 = new Usuario
            {
                Id = "5",
                UsuarioId = "nelapin2",
                LastName = "Pindakova",
                FirstName = "nelapin",
                Email = "nelapin@contoso.com",
                OrderHistory = new OrderHistory[]
                   {
                        new OrderHistory
                        {
                            OrderId = "1001",
                            DateShipped = "08/17/2018",
                            Total = "106.9"
                        }
                    },
                ShippingPreference = new ShippingPreference[]
                {
                    new ShippingPreference
                    {
                        Priority = 2,
                        AddressLine1 = "90 W 82th St",
                        City = "New York",
                        State = "NY",
                        ZipCode = "10001",
                        Country = "USA"
                    }
                },
                CuponUsed = new CuponUsed[]
                {
                    new CuponUsed{ CouponCode ="Fall2018"}
                }
            };
            await this.CreateUserDocumentIfNotExist(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, nelapin);


            yanhe.FirstName = "Cambiado por dormida";
            await this.ReplaceUserDocument(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, yanhe);

        }

        /// <summary>
        /// Insert un documento si existe
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="usuario"></param>
        /// <returns></returns>
        private async Task CreateUserDocumentIfNotExist(string databaseName, string collectionName, Usuario usuario)
        {
            try
            {
                Document doc = await this.client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, usuario.Id), new RequestOptions { PartitionKey = new PartitionKey(usuario.UsuarioId) });
                this.WriteToConsoleAndPromptToContinue("Usuario {0} already exist in the database", usuario.Id);

            }
            catch (DocumentClientException de)
            {
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), usuario);
                    this.WriteToConsoleAndPromptToContinue("Usuario {0} created:", usuario.Id);
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Reads el documento si existe
        /// </summary>
        /// <param name="databaseNames"></param>
        /// <param name="collectionNames"></param>
        /// <param name="usuario"></param>
        /// <returns></returns>
        private async Task ReadIfDocument(string databaseNames, string collectionNames, Usuario usuario)
        {
            try
            {
                await this.client.ReadDocumentAsync(UriFactory.CreateDocumentUri(databaseNames,collectionNames,usuario.Id), new RequestOptions { PartitionKey = new PartitionKey(usuario.UsuarioId)});
                this.WriteToConsoleAndPromptToContinue("Usuario {0} encontrado: ", usuario);
            }
            catch (DocumentClientException de)
            {
                if(de.StatusCode == HttpStatusCode.NotFound)
                {
                    this.WriteToConsoleAndPromptToContinue("Usuario {0} no encontrado: ", usuario.Id);
                }
            }
        }
        
        /// <summary>
        /// Update the specified document
        /// </summary>
        /// <param name="dataBaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="updatedUser"></param>
        /// <returns></returns>
        private async Task ReplaceUserDocument(string dataBaseName, string collectionName, Usuario updatedUser)
        {
            try
            {
                await this.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(EnumDatosBasicos.DataBaseId, EnumDatosBasicos.CollectionId, updatedUser.Id), updatedUser, new RequestOptions { PartitionKey = new PartitionKey(updatedUser.UsuarioId) });
                this.WriteToConsoleAndPromptToContinue("Usuario {0} ACTUALIZADO: ", updatedUser.Id);
            }
            catch(DocumentClientException de)
            {
                if(de.StatusCode == HttpStatusCode.NotFound)
                {
                    this.WriteToConsoleAndPromptToContinue("Usuario {0} no encontrado para ser actualizado: ", updatedUser.Id);
                }
            }
        }
        
        /// <summary>
        /// delete a document if exists
        /// </summary>
        /// <param name="dataBaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="deleteUser"></param>
        /// <returns></returns>
        private async Task DeleteUserDocument(string dataBaseName, string collectionName, Usuario deleteUser)
        {
            try
            {
                await this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(dataBaseName, collectionName, deleteUser.Id), new RequestOptions { PartitionKey = new PartitionKey(deleteUser.UsuarioId) });
                this.WriteToConsoleAndPromptToContinue("DELETED user {0}:", deleteUser.Id);
            }
            catch (DocumentClientException de)
            {
                if(de.StatusCode == HttpStatusCode.NotFound)
                {
                    this.WriteToConsoleAndPromptToContinue("Usuario {0} no se encontró para ser eliminado.", deleteUser.Id);
                }
            }
        } 
        
        /// <summary>
        /// Create a reading query
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="collectionName"></param>
        private void ExecuteSimpleQuery(string databaseName,string collectionName)
        {
            //Set some query options
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };

            //Querying, generates a queriyable instance of the object user to be qeryed into the database
            IQueryable<Usuario> usuarioQuery = this.client.CreateDocumentQuery<Usuario>(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), queryOptions);

            // Defyning query with lambda notation
            usuarioQuery.Where(j => j.LastName == "Pindakova");
            
            Console.WriteLine("Running direct SQL query...   ...");
            foreach(Usuario usuario in usuarioQuery)
            {
                Console.WriteLine("\t Read{0} : ", usuario);
            }

            string query = "SELECT * FROM Usuarios WHERE Usuarios.LastName = 'Pindakova'";

            //Now defyning a Query via SQL
            IQueryable<Usuario> usuarioQueryInSql = this.client.CreateDocumentQuery<Usuario>(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),query,queryOptions);

            // Executing query
            Console.WriteLine("Running direct SQL query...");
            foreach(Usuario usuario1 in usuarioQueryInSql)
            {
                Console.WriteLine("\t Read {0}", usuario1);
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }

        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

    }
}

