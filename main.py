# am ales sa folosesc metoda de programare paralela in care creez mai multe procese
# pentru a aduna consumul de energie din mai multi ani pentru mai multe regiuni din africa dintr-un dataset
# de pe internet

# programul citeste fisierul excel si pentru fiecare regiune stocheaza
# numele industriei energetic cat si valorile de pe toti anii
# dupa ce datele sunt preluate, folosesc un numar de procese default (4)
# careia le sunt atribuite o lista cu numere pentru o industrie energetica anume

# de exemplu, daca industria carbunelui are o lista cu valori energetice de marime 16 (adica valorile energetice din 16 ani),
# cate 4 valori vor fi impartite in 4 procese pentru a calcula valoarea totala a
# consumului de energie pentru industria respectiva

# am incercat sa creez o sarcina egala pentru procese, asa ca verific inainte dimensiunea listei
# dupa ce toate valorile pentru toate industriile energetice sunt calculate si trecute
# intr-un fisier .txt cu numele regiunii, la sfarsitul fiserului este trecut consumul total de energie
# pentru toate industriile din toti anii

# chiar daca nu este cea mai rapida sau eficienta metoda de a calcula aceste date
# am ales sa folosesc metoda asta pentru a demonstra folosirea algoritmului de suma paralela
# intr-o aplicatie

from multiprocessing import Process, Queue
import os
import xlrd

INPUT_FOLDER = 'data'
OUTPUT_FOLDER = 'output'

PROCESS_NUMBER = 4

africa_regions = []

"""
    africa_regions list pattern:
    
    [{
        'region': '',           -> numele regiunii
        'indicators': [         -> lista cu toti indicatorii de energie
            'name': ''          -> numele industriei
            'values': []        -> o lista cu valorile din toti anii pentru regiunea
                                            si industria respectiva 
        ]
    }]
"""

# in aceasta functie calculam suma pentru procesul dat in parametrii


def parallelSum(numbers, queue):
    local_sum = sum(numbers)
    queue.put(local_sum)


# in aceasta functie citim toate fisierele din folderul in care se afla datele
def readData():
    # listam folderul unde se afla datasetul
    files = os.listdir(INPUT_FOLDER)

    # citim fiecare fisier
    for file in files:
        workbook = xlrd.open_workbook(f'{INPUT_FOLDER}/' + file)
        worksheet = workbook.sheet_by_index(0)

        # citim fiecare rand din fisier, exceptand primul care reprezinta labelurile
        for i in range(1, worksheet.nrows):
            current_region = worksheet.cell_value(i, 3)
            current_value = worksheet.cell_value(i, 6)
            current_indicator = worksheet.cell_value(i, 1)

            region_pos = -1
            indicator_pos = -1

            # incepem verificarea regiunii
            for x in range(len(africa_regions)):
                region_pos = -1

                # verificam daca regiunea exista in lista
                # daca aceasta exista, salvam pozitia
                if current_region == africa_regions[x]['region']:
                    region_pos = x

                    # incepem verificarea pentru indicatorul de energie
                    for y in range(len(africa_regions[region_pos]['indicator'])):
                        indicator_pos = -1

                        # daca industrie de energie exista deja pentru regiunea respectiva,
                        # adaugam valorile si salvam pozitia
                        if africa_regions[region_pos]['indicator'][y]['name'] == current_indicator:
                            indicator_pos = y

                            africa_regions[region_pos]['indicator'][indicator_pos]['values'].append(
                                current_value)

                            break
                    break

            # in cazul in care regiunea nu exista in lista o adaugam
            if region_pos == -1:
                data = {
                    'region': '',
                    'indicator': []
                }

                data['region'] += current_region
                africa_regions.append(data)

            # in cazul in care industria de energie nu exista in lista o adaugam
            if indicator_pos == -1:
                data = {
                    'region': '',
                    'indicator': []
                }

                africa_regions[region_pos]['indicator'].append(
                    {'name': current_indicator, 'values': [current_value]})

    # afisam industriile de consum pentru fiecare tara chiar daca acestea nu au loc in consola
    for x in range(len(africa_regions)):
        print(africa_regions[x]['region'], end='\n')
        for y in range(len(africa_regions[x]['indicator'])):
            dicts = africa_regions[x]['indicator'][y]
            print(f"{y}. {dicts['name']}: {dicts['values']}", end='\n')
        print('')


def saveData():
    # verificam daca fisierul de output exista, daca nu il cream
    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)

    # incepem verificarea pentru fiecare regiune
    for x in range(len(africa_regions)):
        # daca fisierul .txt cu datele nu exista, il cream
        # iar daca exista doar il deschidem pentru a evita erorile
        file = None
        if not os.path.exists(f"{OUTPUT_FOLDER}/{africa_regions[x]['region']}.txt"):
            file = open(
                f"{OUTPUT_FOLDER}/{africa_regions[x]['region']}.txt", "x")
            file.close()

        file = open(
            f"{OUTPUT_FOLDER}/{africa_regions[x]['region']}.txt", "w")

        # in cazul in care fisierul exista,
        # folosim algoritmul de suma paralela pentru a calcula consumul total de energie pentru o industrie
        if file != None:
            # incepem adunarea datelor pentru regiunea respectiva pentru fiecare industrie in parte
            # folosind 4 procese pentru fiecare regiune in parte
            processes = PROCESS_NUMBER

            # cream o variabila unde vom stoca suma totala pentru toate industriile
            indicatorMax = 0.0

            # verificam daca numarul de procese este mai mare decat numarul de valori
            # pentru a evita erorile, daca este mai setam numarul de procese
            # egal cu jumatatea marimii listei de valori
            # aceeasi situatie daca numarul de valuri divizibil cu numarul de procese
            # are un rest mai mare de 2 il impartim la 2 pentru a avea o
            # sarcina egala pentru procese
            for y in range(len(africa_regions[x]['indicator'])):

                if len(africa_regions[x]['indicator'][y]['values']) < processes or len(africa_regions[x]['indicator']) % processes > 2:
                    processes = len(
                        africa_regions[x]['indicator'][y]['values']) / 2

                # verificam daca numarul de procese este 0 pentru a evita erorile
                if int(processes) == 0:
                    processes = 1
                else:
                    processes = int(processes)

                process_list = []

                queue = Queue()

                # impartim lista in numarul de procese
                chunks = [africa_regions[x]['indicator'][y]['values'][i::processes]
                          for i in range(processes)]

                # apelam constructorul pentru crearea obiectului
                # obiectul il bagam in lista pentru a putea fii pornit
                for i in range(processes):
                    process_list.append(
                        Process(target=parallelSum, args=(chunks[i], queue)))

                # pornim toate procesele in acelasi timp
                for process in process_list:
                    process.start()

                # declaram o variabila care va reprezenta suma totala a valorilor pentru o industrie energetica
                total_sum = 0.0

                # calculam valorile industriei respectiva
                for process in process_list:
                    total_sum += queue.get()
                    process.join()

                print(
                    f"[{africa_regions[x]['region']}] Total sum for {africa_regions[x]['indicator'][y]['name']} is {total_sum}")

                # scriem in fisier in momentul in care valorile au fost calculate
                # pentru o industrie energetica
                file.write(
                    f"{africa_regions[x]['indicator'][y]['name']}: {total_sum}\n")

                # adaugam suma industriei la valoarea totala de consum de energie pentru regiune
                indicatorMax += total_sum

                # verificam daca industria actuala este ultima din lista noastra
                # si scriem la sfarsitul fisierului consumul total de energie
                # si inchidem fisierul
                if y == len(africa_regions[x]['indicator']) - 1:
                    file.write(
                        f"\nTotal energy consumption for this region: {indicatorMax}")
                    file.close()
                    print('')
            # returnam o eroare daca fisierul nu a putut fii creat
        else:
            print(
                f'Fisierul "{OUTPUT_FOLDER}/{africa_regions[x]["region"]}.txt" nu a fost gasit!')


if __name__ == "__main__":
    readData()
    saveData()
